"""Parse ``workflow.xml`` text into :mod:`flowx.sources.oozie.loader.ast` nodes.

Namespaces are ignored on purpose: CDP estates mix workflow schemas 0.2-1.0 with action extension schemas
(``uri:oozie:spark-action:1.0`` and friends), and the element names are what carry the meaning.
"""

from __future__ import annotations

import re
import xml.etree.ElementTree as ElementTree

from flowx.sources.oozie.loader.ast import (
    ActionNode,
    ActionPayload,
    DecisionCase,
    DecisionNode,
    EndNode,
    ForkNode,
    GenericAction,
    JoinNode,
    KillNode,
    Node,
    OozieParseError,
    OozieWorkflow,
    PrepareStep,
    SparkAction,
    StartNode,
)

_WORKFLOW_NAMESPACE_PREFIX = "uri:oozie:workflow:"
_ACTION_CONTROL_CHILDREN = frozenset({"ok", "error", "info"})
# The XSD's IDENTIFIER type, ``([a-zA-Z_]([\-_a-zA-Z0-9])*){1,39}``, written without the nested repetition: it
# accepts exactly the same names, but can't backtrack exponentially on a long invalid one.
_NODE_NAME = re.compile(r"[a-zA-Z_][-_a-zA-Z0-9]*")


def _local_name(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _children(element: ElementTree.Element, name: str) -> list[ElementTree.Element]:
    return [child for child in element if _local_name(child.tag) == name]


def _child(element: ElementTree.Element, name: str) -> ElementTree.Element | None:
    return next(iter(_children(element, name)), None)


def _text(element: ElementTree.Element, name: str) -> str | None:
    child = _child(element, name)
    return None if child is None else (child.text or "").strip()


def _texts(element: ElementTree.Element, name: str) -> list[str]:
    return [(child.text or "").strip() for child in _children(element, name)]


def _properties(element: ElementTree.Element | None) -> dict[str, str | None]:
    """Reads ``<property><name/><value/></property>`` lists; a missing ``<value>`` becomes ``None``."""
    values: dict[str, str | None] = {}
    if element is None:
        return values
    for property_element in _children(element, "property"):
        name = _text(property_element, "name")
        if name:
            values[name] = _text(property_element, "value")
    return values


def _without_namespaces(element: ElementTree.Element) -> ElementTree.Element:
    """Copies an element tree under its local names, so serialized XML reads the way the author wrote it."""
    copy = ElementTree.Element(
        _local_name(element.tag), {_local_name(key): value for key, value in element.attrib.items()}
    )
    copy.text = element.text
    copy.extend(_without_namespaces(child) for child in element)
    return copy


def _raw_xml(element: ElementTree.Element) -> str:
    return ElementTree.tostring(_without_namespaces(element), encoding="unicode")


def _configuration(element: ElementTree.Element) -> dict[str, str]:
    return {name: value or "" for name, value in _properties(_child(element, "configuration")).items()}


def _optional_int(value: str | None, attribute: str, node_name: str) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except ValueError as error:
        raise OozieParseError(f"action {node_name!r}: {attribute}={value!r} is not a whole number") from error


def _spark_payload(body: ElementTree.Element) -> SparkAction:
    prepare = _child(body, "prepare")
    steps = (
        [PrepareStep(operation=_local_name(step.tag), path=step.get("path", "")) for step in prepare]
        if prepare is not None
        else []
    )
    return SparkAction(
        jar=_text(body, "jar") or "",
        master=_text(body, "master"),
        mode=_text(body, "mode"),
        spark_name=_text(body, "name"),
        main_class=_text(body, "class"),
        spark_opts=" ".join(_texts(body, "spark-opts")),
        arguments=_texts(body, "arg"),
        files=_texts(body, "file"),
        archives=_texts(body, "archive"),
        prepare=steps,
        configuration=_configuration(body),
        job_xml=_texts(body, "job-xml"),
    )


def _payload(body: ElementTree.Element) -> ActionPayload:
    tag = _local_name(body.tag)
    if tag == "spark":
        return _spark_payload(body)
    return GenericAction(
        element_tag=tag,
        raw_xml=_raw_xml(body),
        configuration=_configuration(body),
        job_xml=_texts(body, "job-xml"),
    )


def _action(element: ElementTree.Element, name: str) -> ActionNode:
    ok_element, error_element = _child(element, "ok"), _child(element, "error")
    if ok_element is None or not ok_element.get("to"):
        raise OozieParseError(f"action {name!r} is missing an <ok> element with a 'to' target")
    if error_element is None or not error_element.get("to"):
        raise OozieParseError(f"action {name!r} is missing an <error> element with a 'to' target")
    bodies = [child for child in element if _local_name(child.tag) not in _ACTION_CONTROL_CHILDREN]
    if len(bodies) != 1:
        raise OozieParseError(f"action {name!r} must contain exactly one action body, found {len(bodies)}")
    return ActionNode(
        name=name,
        action_type=_local_name(bodies[0].tag),
        payload=_payload(bodies[0]),
        ok_to=ok_element.get("to", ""),
        error_to=error_element.get("to", ""),
        retry_max=_optional_int(element.get("retry-max"), "retry-max", name),
        retry_interval_minutes=_optional_int(element.get("retry-interval"), "retry-interval", name),
        raw_xml=_raw_xml(element),
    )


def _decision(element: ElementTree.Element, name: str) -> DecisionNode:
    switch = _child(element, "switch")
    if switch is None:
        raise OozieParseError(f"decision {name!r} has no <switch>")
    default = _child(switch, "default")
    if default is None or not default.get("to"):
        raise OozieParseError(f"decision {name!r} has no <default to=...>")
    cases = [
        DecisionCase(predicate=(case.text or "").strip(), to=case.get("to", "")) for case in _children(switch, "case")
    ]
    return DecisionNode(name=name, cases=cases, default_to=default.get("to", ""))


_NODE_TAGS = frozenset({"end", "kill", "decision", "fork", "join", "action"})


def _node(element: ElementTree.Element) -> Node | None:
    tag = _local_name(element.tag)
    if tag not in _NODE_TAGS:
        return None
    name = element.get("name", "")
    # Node names end up in generated task keys, notebook titles, and code comments, so anything outside the
    # schema's identifier syntax (a newline, a quote) is refused rather than escaped everywhere downstream.
    if not _NODE_NAME.fullmatch(name):
        raise OozieParseError(f"invalid node name {name!r}")
    if tag == "end":
        return EndNode(name=name)
    if tag == "kill":
        return KillNode(name=name, message=_text(element, "message") or "")
    if tag == "decision":
        return _decision(element, name)
    if tag == "fork":
        return ForkNode(name=name, paths=[path.get("start", "") for path in _children(element, "path")])
    if tag == "join":
        return JoinNode(name=name, to=element.get("to", ""))
    if tag == "action":
        return _action(element, name)
    return None


def parse_workflow(xml_text: str) -> OozieWorkflow:
    """Parses one ``workflow.xml`` document into an :class:`OozieWorkflow`.

    Raises:
        OozieParseError: When the text isn't well-formed XML, isn't a ``<workflow-app>``, or breaks the
            structural rules every Oozie workflow follows (one ``<start>``, unique node names, actions with
            both ``<ok>`` and ``<error>``).
    """
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as error:
        raise OozieParseError(f"workflow.xml is not well-formed XML: {error}") from error
    if _local_name(root.tag) != "workflow-app":
        raise OozieParseError(f"expected a <workflow-app> root element, found <{_local_name(root.tag)}>")
    namespace = root.tag[1:].split("}", 1)[0] if root.tag.startswith("{") else ""
    schema_version = (
        namespace.removeprefix(_WORKFLOW_NAMESPACE_PREFIX) if namespace.startswith(_WORKFLOW_NAMESPACE_PREFIX) else None
    )

    start_element = _child(root, "start")
    if start_element is None or not start_element.get("to"):
        raise OozieParseError("workflow has no <start> node")
    global_element = _child(root, "global")
    credentials_element = _child(root, "credentials")

    nodes: dict[str, Node] = {}
    for element in root:
        node = _node(element)
        if node is None:
            continue
        if node.name in nodes:
            raise OozieParseError(f"duplicate node name {node.name!r}")
        nodes[node.name] = node

    return OozieWorkflow(
        name=root.get("name", ""),
        start=StartNode(to=start_element.get("to", "")),
        nodes=nodes,
        schema_version=schema_version,
        parameters=_properties(_child(root, "parameters")),
        global_configuration=_configuration(global_element) if global_element is not None else {},
        global_job_xml=_texts(global_element, "job-xml") if global_element is not None else [],
        credentials=[credential.get("name", "") for credential in _children(credentials_element, "credential")]
        if credentials_element is not None
        else [],
        raw_xml=xml_text,
    )

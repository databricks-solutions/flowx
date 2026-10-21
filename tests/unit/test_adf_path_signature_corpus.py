"""Corpus check of the ADF folder signature (:mod:`flowx.sources.adf.path_signature`) over run-time pieces.

Every run-time piece in a folder -- a parameter, a ForEach item, an activity output, a system value, or any function
call -- must leave exactly one ``<P>`` slot where it sits: it never vanishes, and none of its argument text leaks into
the folder. So a folder reads the same whichever run-time piece it holds, and swapping that piece for ``item().x``
leaves its signature unchanged. Each case writes a ``sales@acctA`` dataset with ``folderPath`` bound to one spelling.

A function over a value the call site bound is one slot too, keyed by that value: two bound values never share a
signature, and neither ever reads as folder text. A new run-time construct belongs in this corpus, not in a
per-value patch to the shape code.
"""

from __future__ import annotations

import re

import pytest

from flowx.models.adf_ast import AdfActivity, AdfDataset, AdfDatasetReference, AdfDefinitions, AdfLinkedService
from flowx.sources.adf.dataset_lineage import activity_data_assets

_LITERAL_FOLDERS = ["landing", "raw/sales", "bronze/orders-2024"]

_RUNTIME_PIECES = [
    "pipeline().parameters.run",
    "item().x",
    "variables('v')",
    "activity('L').output.firstRow.f",
    "pipeline().RunId",
    "pipeline()?.TriggerTime",
    "trigger().startTime",
    "utcnow()",
    "utcNow('yyyy')",
    "rand(1,9)",
    "addDays(utcnow(),1)",
    "formatDateTime(utcnow(),'yyyy')",
    "toLower(item().x)",
    "item()['zone']",
    "string(pipeline().parameters.n)",
    "guid()",
    "pipeline().globalParameters.env",
    "if(equals(item().a,'x'),'p','q')",
]

_SPELLINGS = [
    "concat, literal first",
    "concat, literal last",
    "interpolated, literal first",
    "interpolated, literal last",
]

_DEFINITIONS = AdfDefinitions(
    pipelines=[],
    datasets={
        "ds_sales": AdfDataset(
            name="ds_sales",
            type="DelimitedText",
            properties={
                "typeProperties": {"location": {"fileSystem": "sales", "folderPath": "@dataset().folderPath"}},
                "linkedServiceName": {"referenceName": "ls_acctA"},
            },
        )
    },
    linked_services={
        "ls_acctA": AdfLinkedService(
            name="ls_acctA",
            type="AzureBlobFS",
            properties={"typeProperties": {"url": "https://acctA.dfs.core.windows.net"}},
        )
    },
)


def _spell(spelling: str, folder: str, piece: str) -> tuple[str, str]:
    """The ``folderPath`` putting *piece* beside the literal *folder* in *spelling*, and the shape it should give."""
    return {
        "concat, literal first": (f"@concat('{folder}/', {piece})", f"{folder}/<P>"),
        "concat, literal last": (f"@concat({piece}, '/{folder}')", f"<P>/{folder}"),
        "interpolated, literal first": (f"{folder}/@{{{piece}}}", f"{folder}/<P>"),
        "interpolated, literal last": (f"@{{{piece}}}/{folder}", f"<P>/{folder}"),
    }[spelling]


def _written_signature(folder_path: str) -> str:
    """The signature a Copy gets for writing ``ds_sales`` with ``folderPath`` bound to *folder_path*."""
    reference = AdfDatasetReference(
        reference_name="ds_sales", parameters={"folderPath": {"value": folder_path, "type": "Expression"}}
    )
    _, writes = activity_data_assets(AdfActivity(name="Write", type="Copy", outputs=[reference]), _DEFINITIONS)
    return writes[0].signature


@pytest.mark.parametrize("spelling", _SPELLINGS)
@pytest.mark.parametrize("piece", _RUNTIME_PIECES)
@pytest.mark.parametrize("folder", _LITERAL_FOLDERS)
def test_a_runtime_piece_is_one_slot_where_it_sits(folder: str, piece: str, spelling: str) -> None:
    """The piece leaves one ``<P>`` beside the literal folder, exactly as ``item().x`` does in the same spot."""
    folder_path, expected_shape = _spell(spelling, folder, piece)
    signature = _written_signature(folder_path)

    assert "<P>" in signature
    assert signature == _written_signature(_spell(spelling, folder, "item().x")[0])
    assert signature == f"ST[sales@acctA]/FP[{expected_shape}]/FN[]"


_BOUND_WRAPPERS = [
    "toLower({})",
    "string({})",
    "trim({})",
    "replace({}, '-', '_')",
    "if(equals({}, 'archive'), 'old', {})",
    "toUpper(concat('x', {}))",
]

_BOUND_VALUES = ["orders", "customers", "orders-2024"]


def _bound_signature(folder: str, wrapper: str, spelling: str, value: str) -> str:
    """The signature of a dataset whose folder wraps ``dataset().folderPath`` in *wrapper*, called with *value*."""
    call = wrapper.format(*["dataset().folderPath"] * wrapper.count("{}"))
    location = f"@concat('{folder}/', {call})" if spelling == "concat" else f"{folder}/@{{{call}}}"
    definitions = AdfDefinitions(
        pipelines=[],
        datasets={
            "ds_wrapped": AdfDataset(
                name="ds_wrapped",
                type="DelimitedText",
                properties={
                    "typeProperties": {"location": {"fileSystem": "sales", "folderPath": location}},
                    "linkedServiceName": {"referenceName": "ls_acctA"},
                },
            )
        },
        linked_services=_DEFINITIONS.linked_services,
    )
    reference = AdfDatasetReference(reference_name="ds_wrapped", parameters={"folderPath": value})
    _, writes = activity_data_assets(AdfActivity(name="Write", type="Copy", outputs=[reference]), definitions)
    return writes[0].signature


@pytest.mark.parametrize("spelling", ["concat", "interpolated"])
@pytest.mark.parametrize("wrapper", _BOUND_WRAPPERS)
@pytest.mark.parametrize("folder", _LITERAL_FOLDERS)
def test_a_function_over_a_bound_value_is_one_slot_keyed_by_that_value(
    folder: str, wrapper: str, spelling: str
) -> None:
    """Different bound values never share a signature, and the call never reads as folder text or a literal folder."""
    signatures = {value: _bound_signature(folder, wrapper, spelling, value) for value in _BOUND_VALUES}

    assert len(set(signatures.values())) == len(_BOUND_VALUES)
    for value, signature in signatures.items():
        assert re.sub(r"<P:(?:[^<>]|<P>)*>", "<P>", signature) == f"ST[sales@acctA]/FP[{folder}/<P>]/FN[]"
        assert signature != _written_signature(f"{folder}/{value}")

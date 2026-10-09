"""Motif detection and collapsing for ADF pipeline patterns."""

from flowx.motifs.collapser import collapse_motifs, inline_collapsed_lookup_references
from flowx.motifs.detector import detect_motifs

__all__ = ["collapse_motifs", "detect_motifs", "inline_collapsed_lookup_references"]

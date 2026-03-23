"""sources sub-package for the media domain — exposes all source classes."""
from sqldim.application.datasets.domains.media.sources.movies import MoviesSource, _MOVIES_SPEC

__all__ = [
    "MoviesSource",
    "_MOVIES_SPEC",
]

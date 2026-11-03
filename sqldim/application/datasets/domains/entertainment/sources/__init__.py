"""entertainment domain sources."""

from sqldim.application.datasets.domains.entertainment.sources.movies import (
    MoviesSource,
    _MOVIES_SPEC,
)
from sqldim.application.datasets.domains.entertainment.sources.social_network import (
    SocialNetworkSource,
)

__all__ = ["MoviesSource", "SocialNetworkSource", "_MOVIES_SPEC"]

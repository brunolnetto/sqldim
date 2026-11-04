"""entertainment domain — merged media (movies) + social_network (book club) domain."""

from sqldim.application.datasets.domains.entertainment.sources import (
    MoviesSource,
    SocialNetworkSource,
)

__all__ = ["MoviesSource", "SocialNetworkSource"]

from enum import Enum


class DownloadPolicy(Enum):
    ALWAYS = "always"
    IF_MISSING = "if_missing"
    NEVER = "never"
class SplitPolicy(Enum):
    REUSE = "reuse"
    RECREATE = "recreate"
# src/policy/model.py
from dataclasses import dataclass, field
from typing import List, Dict, Optional


@dataclass
class Normalization:
    strip_params: List[str] = field(default_factory=list)  # supports wildcards like utm_*
    collapse_www: bool = True
    drop_fragments: bool = True
    default_port_strip: bool = True
    path_allow_prefixes: List[str] = field(default_factory=list)
    path_deny_regexes: List[str] = field(default_factory=list)


@dataclass
class Discovery:
    static_max: Optional[int] = None
    dyn_max: Optional[int] = None
    include_hosts: List[str] = field(default_factory=list)
    sitemap_hints: List[str] = field(default_factory=list)


@dataclass
class HydrationRenderIf:
    thin_html_min_text_chars: Optional[int] = None
    script_ratio_over: Optional[float] = None


@dataclass
class Hydration:
    render_on_status: List[int] = field(default_factory=list)
    render_if: HydrationRenderIf = field(default_factory=HydrationRenderIf)
    timeout_s: Optional[int] = None
    render_timeout_s: Optional[int] = None
    headers: Dict[str, str] = field(default_factory=dict)


@dataclass
class Cleaning:
    remove_selectors: List[str] = field(default_factory=list)
    keep_main_like: List[str] = field(default_factory=list)


@dataclass
class Chunking:
    size: Optional[int] = None
    overlap: Optional[int] = None


@dataclass
class Backoff:
    base_seconds: float = 0.3
    jitter_seconds: float = 0.0
    on_429_multiplier: float = 2.0


@dataclass
class Policy:
    host: str = "default"
    aliases: List[str] = field(default_factory=list)
    normalization: Normalization = field(default_factory=Normalization)
    discovery: Discovery = field(default_factory=Discovery)
    hydration: Hydration = field(default_factory=Hydration)
    cleaning: Cleaning = field(default_factory=Cleaning)
    chunking: Chunking = field(default_factory=Chunking)
    tagging: Dict[str, List[str]] = field(default_factory=dict)
    backoff: Backoff = field(default_factory=Backoff)
    version: str = "2025-08-31"  # bump when you change YAML

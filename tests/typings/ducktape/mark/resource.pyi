from _typeshed import Incomplete
from ducktape.mark._mark import Mark as Mark

CLUSTER_SPEC_KEYWORD: str
CLUSTER_SIZE_KEYWORD: str

class ClusterUseMetadata(Mark):
    metadata: Incomplete
    def __init__(self, **kwargs) -> None: ...
    @property
    def name(self): ...
    def apply(self, seed_context, context_list): ...

def cluster(**kwargs): ...

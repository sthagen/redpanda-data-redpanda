from _typeshed import Incomplete
from ducktape.utils.util import package_is_installed as package_is_installed

class TemplateRenderer:
    def render_template(self, template, **kwargs): ...
    template_loader: Incomplete
    template_env: Incomplete
    def render(self, path, **kwargs): ...

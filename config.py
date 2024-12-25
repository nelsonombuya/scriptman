from dynaconf import Dynaconf

settings = Dynaconf(settings_files=["settings.toml", ".secrets.toml"])

__all__: list[str] = ["settings"]

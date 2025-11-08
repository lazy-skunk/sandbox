from workspace.load_env_val.sample_base_settings import SampleBaseSettings


def main() -> None:
    settings = SampleBaseSettings()
    print(settings.model_dump_json(indent=2))


if __name__ == "__main__":
    main()

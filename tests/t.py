from dataclasses import dataclass

DEFAULT_KWARGS = dict(slots=True, frozen=True)


@dataclass(**DEFAULT_KWARGS)
class HubUser:
    h_user_pk: str
    user_id: str
    load_dt: str
    load_src: str


def main():
    h = HubUser(h_user_pk=".sdf", user_id="2003992j", load_dt="di2", load_src="fhjisf")

    print(h)


if __name__ == "__main__":
    main()

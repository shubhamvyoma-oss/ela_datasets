"""Atomically rotate edmingle.api_key inside the shared ../credentials.yaml
after a new key has been generated and validated.

Uses a targeted regex replace on the raw file text -- not a
load/yaml.safe_dump round-trip -- specifically so credentials.yaml's
comments and formatting survive untouched. Every other pipeline under
ela_datasets/ reads that same file, so this is the one place allowed to
write to it.
"""

from __future__ import annotations

import os
import re

_API_KEY_LINE = re.compile(r'^(\s*api_key:\s*)"[^"]*"(.*)$', re.MULTILINE)


class CredentialsUpdateError(RuntimeError):
    """The shared credentials.yaml could not be updated with the new key."""


def update_shared_api_key(credentials_path: str, new_api_key: str) -> None:
    if not new_api_key or any(character.isspace() for character in new_api_key):
        raise CredentialsUpdateError("refusing to write an empty/whitespace api key")

    try:
        with open(credentials_path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError as error:
        raise CredentialsUpdateError(
            f"could not read {credentials_path}: {error}"
        ) from error

    new_content, replaced = _API_KEY_LINE.subn(
        lambda match: f'{match.group(1)}"{new_api_key}"{match.group(2)}',
        content,
        count=1,
    )
    if replaced != 1:
        raise CredentialsUpdateError(
            f"could not find a single 'api_key: \"...\"' line in {credentials_path} "
            "-- refusing to write, file may have been restructured"
        )

    tmp_path = f"{credentials_path}.tmp"
    try:
        with open(tmp_path, "w", encoding="utf-8") as f:
            f.write(new_content)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path, credentials_path)
    except OSError as error:
        raise CredentialsUpdateError(
            f"could not write updated credentials.yaml: {error}"
        ) from error
    finally:
        if os.path.exists(tmp_path):
            try:
                os.remove(tmp_path)
            except OSError:
                pass

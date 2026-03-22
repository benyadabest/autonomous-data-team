from __future__ import annotations

import os

from autonomous_data_team.crewai_bridge import _prepare_crewai_environment


def test_prepare_crewai_environment_redirects_home(settings) -> None:
    original_home = os.environ.get("HOME")
    try:
        _prepare_crewai_environment(settings)
        assert os.environ["HOME"] == str(settings.crewai_home_dir)
        assert os.environ["CREWAI_DISABLE_TELEMETRY"] == "true"
        assert os.environ["CREWAI_DISABLE_TRACKING"] == "true"
        assert os.environ["CREWAI_TRACING_ENABLED"] == "false"
        assert os.environ["OTEL_SDK_DISABLED"] == "true"
    finally:
        if original_home is None:
            os.environ.pop("HOME", None)
        else:
            os.environ["HOME"] = original_home

from unittest import mock

import pytest

from windnf import cli


def test_split_comma_separated():
    action = cli.SplitCommaSeparated()

    class Namespace:
        pass

    namespace = Namespace()
    action(None, namespace, "a,b , c", None)
    assert getattr(namespace, action.dest) == ["a", "b", "c"]


def test_highlight_matches_basic():
    text = "bash-coreutils"
    patterns = ["bash", "core"]
    result = cli.highlight_matches(text, patterns)
    # Should highlight the patterns with color codes
    assert "\033[" in result


@mock.patch("windnf.cli.DbManager")
@mock.patch("windnf.cli.Downloader")
@mock.patch("windnf.cli.MetadataManager")
@mock.patch("windnf.cli.add_repo")
def test_repoadd_command(mock_add_repo, mock_metadata_manager, mock_downloader, mock_db_manager):
    config = mock.Mock()
    args = mock.Mock(command="repoadd", name="testrepo", baseurl="http://example.com", repomd="repodata/repomd.xml")
    cli.handle_command(args, config)
    mock_add_repo.assert_called_once_with(
        mock_db_manager.return_value, "testrepo", "http://example.com", "repodata/repomd.xml"
    )

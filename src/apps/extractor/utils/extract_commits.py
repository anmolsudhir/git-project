import sys
import os
from datetime import datetime
from pandas import DataFrame
from typing import Any, Dict, Optional, List
from pydriller import Repository


class Extractor:
    """
    A class to extract and export git commit information from a repository.

    This class provides functionality to:
    1. Extract commits from a git repository
    2. Convert commits to a DataFrame
    3. Export commits in different formats (parquet, csv)

    Attributes:
        type (str): Export file type (parquet or csv)
        repo_url (str): URL or path to the git repository
        commits (List[Dict[str, Any]]): List of extracted commit dictionaries
        commit_data_frame (Optional[DataFrame]): DataFrame containing commit data
        parent_path (str): Current working directory
        export_path (Optional[str]): Path where export file is saved
    """

    def __init__(self, type: str, today: bool, repo_url: str):
        """
        Initialize the Extractor with repository and export settings.

        Args:
            type (str): Type of export file ('parquet' or 'csv')
            today (bool): If True, extract only commits from today
            repo_url (str): URL or path to the git repository
        """
        self.type = type
        self.repo_url: str | None = repo_url
        self.repo: Optional[Repository] = None
        self.commits: List[Dict[str, Any]] = []
        self.commit_data_frame: Optional[DataFrame] = None
        self.parent_path: str = os.getcwd()
        self.export_path: Optional[str] = None

        # Configure repository based on 'today' flag
        if today:
            _today = datetime.now()
            _start_of_day = datetime(_today.year, _today.month, _today.day)
            self.repo = Repository(self.repo_url, since=_start_of_day)
        else:
            self.repo = Repository(self.repo_url)

    def commit_to_dict(self, commit):
        """
        Convert a PyDriller Commit object to a dictionary.

        Args:
            commit (Commit): PyDriller Commit object

        Returns:
            dict: Comprehensive dictionary representation of the commit
        """
        return {
            # Basic commit metadata
            "hash": commit.hash,
            "msg": commit.msg,
            # Author and committer details (with null safety)
            "author": (
                {"name": commit.author.name, "email": commit.author.email}
                if commit.author
                else None
            ),
            "committer": (
                {"name": commit.committer.name, "email": commit.committer.email}
                if commit.committer
                else None
            ),
            # Timestamp information
            "author_date": commit.author_date,
            "author_timezone": commit.author_timezone,
            "committer_date": commit.committer_date,
            "committer_timezone": commit.committer_timezone,
            # Branch and merge information
            "branches": commit.branches,
            "in_main_branch": commit.in_main_branch,
            "merge": commit.merge,
            # Modified files
            "modified_files": [
                {
                    "filename": mf.filename,
                    "old_path": mf.old_path,
                    "new_path": mf.new_path,
                    "change_type": str(mf.change_type),
                    "added_lines": mf.added_lines,
                    "deleted_lines": mf.deleted_lines,
                    "source_code": mf.source_code,
                    "source_code_before": mf.source_code_before,
                }
                for mf in commit.modified_files
            ],
            # Commit relationships
            "parents": commit.parents,
            # Project information
            "project_name": commit.project_name,
            "project_path": commit.project_path,
            # Modification statistics
            "deletions": commit.deletions,
            "insertions": commit.insertions,
            "lines": commit.lines,
            "files": commit.files,
            # DMM (Design Maintenance Metrics)
            "dmm_unit_size": commit.dmm_unit_size,
            "dmm_unit_complexity": commit.dmm_unit_complexity,
            "dmm_unit_interfacing": commit.dmm_unit_interfacing,
        }

    def _extract_commits(self):
        """
        Extract commits from the repository.

        Raises:
            Exception: If no repository is found or configured
        """
        if not self.repo:
            raise Exception("No repository found or configured.")

        # Extract commits and convert to dictionaries
        for commit in self.repo.traverse_commits():
            self.commits.append(self.commit_to_dict(commit))

    def _create_data_frame(self):
        """
        Create a DataFrame from extracted commits.

        Exits the program if no commits are found.
        """
        if not self.commits:
            print("No commits found, exiting...")
            sys.exit(0)

        self.commit_data_frame = DataFrame(self.commits)

    def _export(self) -> str:
        """
        Export the commit DataFrame to the specified file type.

        Raises:
            Exception: If export type is invalid or export fails

        Returns:
            str: Path to the exported file
        """
        # Validate export type
        if not self.type:
            raise ValueError("No export type provided. Use 'parquet' or 'csv'.")

        if self.commit_data_frame is None:
            raise ValueError("No commit data frame found to export.")

        # Export based on specified type
        if self.type == "parquet":
            export_filename = "commits.parquet.gzip"
            self.commit_data_frame.to_parquet(export_filename, compression="gzip")
            self.export_path = os.path.join(self.parent_path, export_filename)

        elif self.type == "csv":
            export_filename = "commits.csv"
            self.commit_data_frame.to_csv(export_filename)
            self.export_path = os.path.join(self.parent_path, export_filename)

        else:
            raise ValueError(
                f"Unsupported export type: {self.type}. Use 'parquet' or 'csv'."
            )

        return self.export_path

    def extract(self):
        """
        Main method to extract, process, and export commit data.

        Steps:
        1. Extract commits from repository
        2. Create DataFrame from commits
        3. Export DataFrame to specified file type

        Returns:
            str: Path to the exported file
        """
        # 1. Extract Commits
        self._extract_commits()

        # 2. Create DataFrames
        self._create_data_frame()

        # 3. Export to the requested type
        export_path = self._export()

        return export_path

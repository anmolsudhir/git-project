import sys
import os
from datetime import datetime
from pandas import DataFrame
from typing import Optional, List
from pydriller import Commit, Repository


class Extractor:
    def __init__(self, type: str, today: bool, repo_url: str):
        self.type = type
        self.repo_url: str | None = repo_url
        self.repo: Optional[Repository]
        self.commits: List[Commit] = []
        self.commit_data_frame: Optional[DataFrame]
        self.parent_path: str = os.getcwd()
        self.export_path: Optional[str]

        if today:
            _today = datetime.now()
            _start_of_day = datetime(_today.year, _today.month, _today.day)
            self.repo = Repository(self.repo_url, since=_start_of_day)
        self.repo = Repository(self.repo_url)

    def _extract_commits(self):
        if not self.repo:
            raise Exception("No repository found.")

        for commit in self.repo.traverse_commits():
            self.commits.append(commit)

    def _create_data_frame(self):
        if not self.commits:
            print("No commits found, exiting...")
            sys.exit(0)

        self.commit_data_frame = DataFrame(self.commits)

    def _export(self) -> str:
        if not self.type:
            raise Exception("No export type provided.")

        if self.commit_data_frame is None:
            raise Exception("No export commit data frame found.")

        if self.type == "parquet":
            self.commit_data_frame.to_parquet(
                "commits.parquet.gzip", compression="gzip"
            )
            self.export_path = os.path.join(self.parent_path, "commits.parquet.gzip")

        if self.type == "csv":
            self.commit_data_frame.to_csv("commits.csv")
            self.export_path = os.path.join(self.parent_path, "commits.csv")

        if not self.export_path:
            raise Exception(f"Error: Could not export to type {self.type}")

        return self.export_path

    def extract(self):
        # 1. Extract Commits
        self._extract_commits()

        # 2. Create DataFrames
        self._create_data_frame()

        # 3. Export to the requested type
        self._export()

        return self.export_path

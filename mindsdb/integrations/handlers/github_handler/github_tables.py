import pandas as pd
from typing import List
from mindsdb.integrations.libs.api_handler import APITable
from mindsdb.integrations.handlers.utilities.query_utilities import SELECTQueryParser, SELECTQueryExecutor
from mindsdb.integrations.utilities.sql_utils import extract_comparison_conditions
from mindsdb.utilities.log import get_log
from mindsdb_sql.parser import ast

logger = get_log("integrations.github_handler")


class GithubIssuesTable(APITable):
    """The GitHub Issue Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository issues" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            GitHub issues matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        total_results = query.limit.value if query.limit else 20
        issues_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "issues":
                    next

                if an_order.field.parts[1] in ["created", "updated", "comments"]:
                    if issues_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for created/updated/comments"
                        )

                    issues_kwargs["sort"] = an_order.field.parts[1]
                    issues_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "state":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")
                if a_where[2] not in ["open", "closed", "all"]:
                    raise ValueError(
                        f"Unsupported where argument for state {a_where[2]}"
                    )

                issues_kwargs["state"] = a_where[2]

                continue
            if a_where[1] == "labels":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for labels")

                issues_kwargs["labels"] = a_where[2].split(",")

                continue
            if a_where[1] not in ["assignee", "creator"]:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

            if a_where[0] != "=":
                raise ValueError(f"Unsupported where operation for {a_where[1]}")

            issues_kwargs[a_where[1]] = a_where[2]
        self.handler.connect()

        github_issues_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for an_issue in self.handler.connection.get_repo(
                    self.handler.repository
                ).get_issues(**issues_kwargs)[start : start + 10]:
                    if an_issue.pull_request:
                        continue

                    logger.debug(f"Processing issue {an_issue.number}")

                    github_issues_df = pd.concat(
                        [
                            github_issues_df,
                            pd.DataFrame(
                                [
                                    {
                                        "number": an_issue.number,
                                        "title": an_issue.title,
                                        "state": an_issue.state,
                                        "creator": an_issue.user.login,
                                        "closed_by": an_issue.closed_by.login
                                        if an_issue.closed_by
                                        else None,
                                        "labels": ",".join(
                                            [label.name for label in an_issue.labels]
                                        ),
                                        "assignees": ",".join(
                                            [
                                                assignee.login
                                                for assignee in an_issue.assignees
                                            ]
                                        ),
                                        "comments": an_issue.comments,
                                        "body": an_issue.body,
                                        "created": an_issue.created_at,
                                        "updated": an_issue.updated_at,
                                        "closed": an_issue.closed_at,
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_issues_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_issues_df.shape[0] >= total_results:
                break
            else:
                start += 10

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(github_issues_df) == 0:
            github_issues_df = pd.DataFrame([], columns=selected_columns)
        else:
            github_issues_df.columns = self.get_columns()
            for col in set(github_issues_df.columns).difference(set(selected_columns)):
                github_issues_df = github_issues_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                github_issues_df = github_issues_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return github_issues_df

    def insert(self, query: ast.Insert):
        """Inserts data into the GitHub "Create an issue" API

        Parameters
        ----------
        query : ast.Insert
           Given SQL INSERT query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        if self.handler.connection_data.get("api_key", None) is None:
            raise ValueError(
                "Need an authenticated connection in order to insert a GitHub issue"
            )

        self.handler.connect()
        current_repo = self.handler.connection.get_repo(self.handler.repository)

        columns = [col.name for col in query.columns]

        supported_columns = {"title", "body", "assignees", "milestone", "labels"}

        if not set(columns).issubset(supported_columns):
            unsupported_columns = set(columns).difference(supported_columns)
            raise ValueError(
                "Unsupported columns for GitHub issue insert: "
                + ", ".join(unsupported_columns)
            )

        for a_row in query.values:
            insert_kwargs = {}
            a_value = dict(zip(columns, a_row))

            if a_value.get("title", None) is None:
                raise ValueError("Title parameter is required to insert a GitHub issue")

            if a_value.get("body", None):
                insert_kwargs["body"] = a_value["body"]

            if a_value.get("assignees", None):
                insert_kwargs["assignees"] = []
                for an_assignee in a_value["assignees"].split(","):
                    an_assignee = an_assignee.replace(" ", "")
                    try:
                        github_user = self.handler.connection.get_user(an_assignee)
                    except Exception as e:
                        raise ValueError(
                            f'Encountered an exception looking up assignee "{an_assignee}" in GitHub: '
                            f"{type(e).__name__} - {e}"
                        )

                    insert_kwargs["assignees"].append(github_user)

            if a_value.get("milestone", None):
                current_milestones = current_repo.get_milestones()

                found_existing_milestone = False
                for a_milestone in current_milestones:
                    if a_milestone.title == a_value["milestone"]:
                        insert_kwargs["milestone"] = a_milestone
                        found_existing_milestone = True
                        break

                if not found_existing_milestone:
                    logger.debug(
                        f"Milestone \"{a_value['milestone']}\" not found, creating it"
                    )
                    insert_kwargs["milestone"] = current_repo.create_milestone(
                        a_value["milestone"]
                    )
                else:
                    logger.debug(f"Milestone \"{a_value['milestone']}\" already exists")

            if a_value.get("labels", None):
                insert_kwargs["labels"] = []

                inserted_labels = []
                for a_label in a_value["labels"].split(","):
                    a_label = a_label.replace(" ", "")
                    inserted_labels.append(a_label)

                existing_labels = current_repo.get_labels()

                existing_labels_set = {label.name for label in existing_labels}

                if not set(inserted_labels).issubset(existing_labels_set):
                    new_inserted_labels = set(inserted_labels).difference(
                        existing_labels_set
                    )
                    logger.debug(
                        "Inserting new labels: " + ", ".join(new_inserted_labels)
                    )
                    for a_new_label in new_inserted_labels:
                        current_repo.create_label(a_new_label, "000000")

                for a_label in existing_labels:
                    if a_label.name in inserted_labels:
                        insert_kwargs["labels"].append(a_label)

            try:
                current_repo.create_issue(a_value["title"], **insert_kwargs)
            except Exception as e:
                raise ValueError(
                    f"Encountered an exception creating an issue in GitHub: "
                    f"{type(e).__name__} - {e}"
                )

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "number",
            "title",
            "state",
            "creator",
            "closed_by",
            "labels",
            "assignees",
            "comments",
            "body",
            "created",
            "updated",
            "closed",
        ]


class GithubPullRequestsTable(APITable):
    """The GitHub Issue Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository pull requests" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame
            GitHub pull requests matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        total_results = query.limit.value if query.limit else 20
        issues_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "pull_requests":
                    next

                if an_order.field.parts[1] in ["created", "updated", "popularity"]:
                    if issues_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for created/updated/popularity"
                        )

                    issues_kwargs["sort"] = an_order.field.parts[1]
                    issues_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] == "long_running":
                    if issues_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for long_running"
                        )

                    issues_kwargs["sort"] = "long-running"
                    issues_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] == "state":
                if a_where[0] != "=":
                    raise ValueError("Unsupported where operation for state")
                if a_where[2] not in ["open", "closed", "all"]:
                    raise ValueError(
                        f"Unsupported where argument for state {a_where[2]}"
                    )

                issues_kwargs["state"] = a_where[2]

                continue
            if a_where[1] not in ["head", "base"]:
                raise ValueError(f"Unsupported where argument {a_where[1]}")

            if a_where[0] != "=":
                raise ValueError(f"Unsupported where operation for {a_where[1]}")

            issues_kwargs[a_where[1]] = a_where[2]
        self.handler.connect()

        github_pull_requests_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:
                for a_pull in self.handler.connection.get_repo(
                    self.handler.repository
                ).get_pulls(**issues_kwargs)[start : start + 10]:

                    github_pull_requests_df = pd.concat(
                        [
                            github_pull_requests_df,
                            pd.DataFrame(
                                [
                                    {
                                        "number": a_pull.number,
                                        "title": a_pull.title,
                                        "state": a_pull.state,
                                        "creator": a_pull.user.login,
                                        "labels": ",".join(
                                            [label.name for label in a_pull.labels]
                                        ),
                                        "milestone": a_pull.milestone.title
                                        if a_pull.milestone
                                        else None,
                                        "assignees": ",".join(
                                            [
                                                assignee.login
                                                for assignee in a_pull.assignees
                                            ]
                                        ),
                                        "reviewers": ",".join(
                                            [
                                                reviewer.login
                                                for reviewer in a_pull.requested_reviewers
                                            ]
                                        ),
                                        "teams": ",".join(
                                            [
                                                team.name
                                                for team in a_pull.requested_teams
                                            ]
                                        ),
                                        "comments": a_pull.comments,
                                        "review_comments": a_pull.review_comments,
                                        "draft": a_pull.draft,
                                        "is_merged": a_pull.merged,
                                        "mergeable": a_pull.mergeable,
                                        "mergeable_state": a_pull.mergeable_state,
                                        "merged_by": a_pull.merged_by.login
                                        if a_pull.merged_by
                                        else None,
                                        "rebaseable": a_pull.rebaseable,
                                        "body": a_pull.body,
                                        "base": a_pull.base.ref
                                        if a_pull.base
                                        else None,
                                        "head": a_pull.head.ref
                                        if a_pull.head
                                        else None,
                                        "commits": a_pull.commits,
                                        "additions": a_pull.additions,
                                        "deletions": a_pull.deletions,
                                        "changed_files": a_pull.changed_files,
                                        "created": a_pull.created_at,
                                        "updated": a_pull.updated_at,
                                        "merged": a_pull.merged_at,
                                        "closed": a_pull.closed_at,
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_pull_requests_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_pull_requests_df.shape[0] >= total_results:
                break
            else:
                start += 10

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(github_pull_requests_df) == 0:
            github_pull_requests_df = pd.DataFrame([], columns=selected_columns)
        else:
            github_pull_requests_df.columns = self.get_columns()
            for col in set(github_pull_requests_df.columns).difference(
                set(selected_columns)
            ):
                github_pull_requests_df = github_pull_requests_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                github_pull_requests_df = github_pull_requests_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return github_pull_requests_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """
        return [
            "number",
            "title",
            "state",
            "creator",
            "labels",
            "milestone",
            "assignees",
            "reviewers",
            "teams",
            "comments",
            "review_comments",
            "draft",
            "is_merged",
            "mergeable",
            "mergeable_state",
            "merged_by",
            "rebaseable",
            "body",
            "base",
            "head",
            "commits",
            "additions",
            "deletions",
            "changed_files",
            "created",
            "updated",
            "merged",
            "closed",
        ]

class GithubCommitsTable(APITable):
    """The GitHub Commits Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List commits" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub commits matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        conditions = extract_comparison_conditions(query.where)

        total_results = query.limit.value if query.limit else 20
        commits_kwargs = {}
        order_by_conditions = {}

        if query.order_by and len(query.order_by) > 0:
            order_by_conditions["columns"] = []
            order_by_conditions["ascending"] = []

            for an_order in query.order_by:
                if an_order.field.parts[0] != "commits":
                    next

                if an_order.field.parts[1] in ["author", "date", "message"]:
                    if commits_kwargs != {}:
                        raise ValueError(
                            "Duplicate order conditions found for author/date/message"
                        )

                    commits_kwargs["sort"] = an_order.field.parts[1]
                    commits_kwargs["direction"] = an_order.direction
                elif an_order.field.parts[1] in self.get_columns():
                    order_by_conditions["columns"].append(an_order.field.parts[1])

                    if an_order.direction == "ASC":
                        order_by_conditions["ascending"].append(True)
                    else:
                        order_by_conditions["ascending"].append(False)
                else:
                    raise ValueError(
                        f"Order by unknown column {an_order.field.parts[1]}"
                    )

        for a_where in conditions:
            if a_where[1] != "author":
                raise ValueError(f"Unsupported where argument {a_where[1]}")

            if a_where[0] != "=":
                raise ValueError("Unsupported where operation for author")
            commits_kwargs["author"] = a_where[2]
        self.handler.connect()

        github_commits_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for a_commit in self.handler.connection.get_repo(
                    self.handler.repository
                ).get_commits(**commits_kwargs)[start : start + 10]:
                    logger.debug(f"Processing commit {a_commit.sha}")

                    github_commits_df = pd.concat(
                        [
                            github_commits_df,
                            pd.DataFrame(
                                [
                                    {
                                        "sha": a_commit.sha,
                                        "author": a_commit.commit.author.name,
                                        "date": a_commit.commit.author.date,
                                        "message": a_commit.commit.message,
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_commits_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_commits_df.shape[0] >= total_results:
                break
            else:
                start += 10

        selected_columns = []
        for target in query.targets:
            if isinstance(target, ast.Star):
                selected_columns = self.get_columns()
                break
            elif isinstance(target, ast.Identifier):
                selected_columns.append(target.parts[-1])
            else:
                raise ValueError(f"Unknown query target {type(target)}")

        if len(github_commits_df) == 0:
            github_commits_df = pd.DataFrame([], columns=selected_columns)
        else:
            github_commits_df.columns = self.get_columns()
            for col in set(github_commits_df.columns).difference(
                set(selected_columns)
            ):
                github_commits_df = github_commits_df.drop(col, axis=1)

            if len(order_by_conditions.get("columns", [])) > 0:
                github_commits_df = github_commits_df.sort_values(
                    by=order_by_conditions["columns"],
                    ascending=order_by_conditions["ascending"],
                )

        return github_commits_df

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return ["sha", "author", "date", "message"]
      
class GithubReleasesTable(APITable):
    """The GitHub Releases Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository releases" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub releases matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'releases',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 20

        self.handler.connect()

        github_releases_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for a_release in self.handler.connection.get_repo(
                    self.handler.repository
                ).get_releases()[start: start + 10]:

                    logger.debug(f"Processing release {a_release.id}")

                    github_releases_df = pd.concat(
                        [
                            github_releases_df,
                            pd.DataFrame(
                                [
                                    {
                                        "id": self.check_none(a_release.id),
                                        "author": self.check_none(a_release.author.login),
                                        "body": self.check_none(a_release.body),
                                        "created_at": self.check_none(str(a_release.created_at)),
                                        "html_url": self.check_none(a_release.html_url),
                                        "published_at": self.check_none(str(a_release.published_at)),
                                        "tag_name": self.check_none(a_release.tag_name),
                                        "title": self.check_none(a_release.title),
                                        "url": self.check_none(a_release.url),
                                        "zipball_url": self.check_none(a_release.zipball_url)
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_releases_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_releases_df.shape[0] >= total_results:
                break
            else:
                start += 10

        select_statement_executor = SELECTQueryExecutor(
            github_releases_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        github_releases_df = select_statement_executor.execute_query()

        return github_releases_df

    def check_none(self, val):
        return "" if val is None else val

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "id",
            "author",
            "body",
            "created_at",
            "html_url",
            "published_at",
            "tag_name",
            "title",
            "url",
            "zipball_url"
        ]
     
class GithubBranchesTable(APITable):
    """The GitHub Branches Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository branches" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub branches matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'branches',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 20

        self.handler.connect()

        github_branches_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for branch in self.handler.connection.get_repo(self.handler.repository).get_branches()[start: start + 10]:
                    logger.debug(f"Processing branch {branch.name}")
                    raw_data = branch.raw_data
                    github_branches_df = pd.concat(
                        [
                            github_branches_df,
                            pd.DataFrame(
                                [
                                    {
                                        "name": self.check_none(raw_data["name"]),
                                        "url": "https://github.com/" + self.handler.repository + "/tree/" + raw_data["name"],
                                        "commit_sha": self.check_none(raw_data["commit"]["sha"]),
                                        "commit_url": self.check_none(raw_data["commit"]["url"]),
                                        "protected": self.check_none(raw_data["protected"])
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_branches_df.shape[0] >= total_results: break
            except IndexError:
                break

            if github_branches_df.shape[0] >= total_results:
                break
            else:
                start += 10

        select_statement_executor = SELECTQueryExecutor(
            github_branches_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        github_branches_df = select_statement_executor.execute_query()

        return github_branches_df

    def check_none(self, val):
        return "" if val is None else val

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "name",
            "url",
            "commit_sha",
            "commit_url",
            "protected"
        ]

class GithubContributorsTable(APITable):
    """The GitHub Contributors Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository contributors" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub contributors matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'contributors',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 20

        self.handler.connect()

        github_contributors_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for contributor in self.handler.connection.get_repo(self.handler.repository).get_contributors()[start: start + 10]:
                    
                    raw_data = contributor.raw_data
                    github_contributors_df = pd.concat(
                        [
                            github_contributors_df,
                            pd.DataFrame(
                                [
                                    {
                                        "avatar_url": self.check_none(raw_data["avatar_url"]),
                                        "html_url": self.check_none(raw_data["html_url"]),
                                        "followers_url": self.check_none(raw_data["followers_url"]),
                                        "subscriptions_url": self.check_none(raw_data["subscriptions_url"]),
                                        "organizations_url": self.check_none(raw_data["organizations_url"]),
                                        "repos_url": self.check_none(raw_data["repos_url"]),
                                        "events_url": self.check_none(raw_data["events_url"]),
                                        "received_events_url": self.check_none(raw_data["received_events_url"]),
                                        "site_admin": self.check_none(raw_data["site_admin"]),
                                        "name": self.check_none(raw_data["name"]),
                                        "company": self.check_none(raw_data["company"]),
                                        "blog": self.check_none(raw_data["blog"]),
                                        "location": self.check_none(raw_data["location"]),
                                        "email": self.check_none(raw_data["email"]),
                                        "hireable": self.check_none(raw_data["hireable"]),
                                        "bio": self.check_none(raw_data["bio"]),
                                        "twitter_username": self.check_none(raw_data["twitter_username"]),
                                        "public_repos": self.check_none(raw_data["public_repos"]),
                                        "public_gists": self.check_none(raw_data["public_repos"]),
                                        "followers": self.check_none(raw_data["followers"]),
                                        "following": self.check_none(raw_data["following"]),
                                        "created_at": self.check_none(raw_data["created_at"]),
                                        "updated_at": self.check_none(raw_data["updated_at"])
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_contributors_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_contributors_df.shape[0] >= total_results:
                break
            else:
                start += 10

        select_statement_executor = SELECTQueryExecutor(
            github_contributors_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        github_contributors_df = select_statement_executor.execute_query()

        return github_contributors_df

    def check_none(self, val):
        return "" if val is None else val

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "avatar_url",
            "html_url",
            "followers_url",
            "subscriptions_url",
            "organizations_url",
            "repos_url",
            "events_url",
            "received_events_url",
            "site_admin",
            "name",
            "company",
            "blog",
            "location",
            "email",
            "hireable",
            "bio",
            "twitter_username",
            "public_repos",
            "public_gists",
            "followers",
            "following",
            "created_at",
            "updated_at"
        ]
        
class GithubProjectsTable(APITable):
    """The GitHub Projects Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository projects" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub projects matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'projects',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 20

        self.handler.connect()

        github_projects_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for project in self.handler.connection.get_repo(self.handler.repository).get_projects()[start: start + 10]:
                    
                    raw_data = project.raw_data
                    github_projects_df = pd.concat(
                        [
                            github_projects_df,
                            pd.DataFrame(
                                [
                                    {
                                        "owner_url": self.check_none(raw_data["owner_url"]),
                                        "url": self.check_none(raw_data["url"]), 
                                        "html_url": self.check_none(raw_data["html_url"]),
                                        "columns_url": self.check_none(raw_data["columns_url"]), 
                                        "id": self.check_none(raw_data["id"]), 
                                        "node_id": self.check_none(raw_data["node_id"]), 
                                        "name": self.check_none(raw_data["name"]), 
                                        "body": self.check_none(raw_data["body"]), 
                                        "number": self.check_none(raw_data["number"]), 
                                        "state": self.check_none(raw_data["state"]), 
                                        "created_at": self.check_none(raw_data["created_at"]), 
                                        "updated_at": self.check_none(raw_data["updated_at"]),
                                        "creator_login": self.check_none(raw_data["creator"]["login"]),
                                        "creator_id": self.check_none(raw_data["creator"]["id"]),
                                        "creator_url": self.check_none(raw_data["creator"]["url"]),
                                        "creator_html_url": self.check_none(raw_data["creator"]["html_url"]),
                                        "creator_site_admin": self.check_none(raw_data["creator"]["site_admin"])
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_projects_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_projects_df.shape[0] >= total_results:
                break
            else:
                start += 10

        select_statement_executor = SELECTQueryExecutor(
            github_projects_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        github_projects_df = select_statement_executor.execute_query()

        return github_projects_df

    def check_none(self, val):
        return "" if val is None else val

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "owner_url", 
            "url",
            "html_url", 
            "columns_url", 
            "id", 
            "node_id",
            "name", 
            "body", 
            "number", 
            "state", 
            "created_at",
            "updated_at", 
            "creator_login", 
            "creator_id", 
            "creator_url",
            "creator_html_url", 
            "creator_site_admin"
        ]
      
class GithubMilestonesTable(APITable):
    """The GitHub Milestones Table implementation"""

    def select(self, query: ast.Select) -> pd.DataFrame:
        """Pulls data from the GitHub "List repository milestones" API

        Parameters
        ----------
        query : ast.Select
           Given SQL SELECT query

        Returns
        -------
        pd.DataFrame

            GitHub milestones matching the query

        Raises
        ------
        ValueError
            If the query contains an unsupported condition
        """

        select_statement_parser = SELECTQueryParser(
            query,
            'milestones',
            self.get_columns()
        )

        selected_columns, where_conditions, order_by_conditions, result_limit = select_statement_parser.parse_query()

        total_results = result_limit if result_limit else 20

        self.handler.connect()

        github_milestones_df = pd.DataFrame(columns=self.get_columns())

        start = 0

        while True:
            try:

                for milestone in self.handler.connection.get_repo(self.handler.repository).get_milestones()[start: start + 10]:
                    
                    raw_data = milestone.raw_data
                    github_milestones_df = pd.concat(
                        [
                            github_milestones_df,
                            pd.DataFrame(
                                [
                                    {
                                        "url": self.check_none(raw_data["url"]),
                                        "html_url": self.check_none(raw_data["html_url"]),
                                        "labels_url": self.check_none(raw_data["labels_url"]),
                                        "id": self.check_none(raw_data["id"]),
                                        "node_id": self.check_none(raw_data["node_id"]),
                                        "number": self.check_none(raw_data["number"]),
                                        "title": self.check_none(raw_data["title"]),
                                        "description": self.check_none(raw_data["description"]),
                                        "creator": self.check_none(raw_data["creator"]),
                                        "open_issues": self.check_none(raw_data["open_issues"]),
                                        "closed_issues": self.check_none(raw_data["closed_issues"]),
                                        "state": self.check_none(raw_data["state"]), 
                                        "created_at": self.check_none(raw_data["created_at"]),
                                        "updated_at": self.check_none(raw_data["updated_at"]),
                                        "due_on": self.check_none(raw_data["due_on"]),
                                        "closed_at": self.check_none(raw_data["closed_at"])
                                    }
                                ]
                            ),
                        ]
                    )

                    if github_milestones_df.shape[0] >= total_results:
                        break
            except IndexError:
                break

            if github_milestones_df.shape[0] >= total_results:
                break
            else:
                start += 10

        select_statement_executor = SELECTQueryExecutor(
            github_milestones_df,
            selected_columns,
            where_conditions,
            order_by_conditions
        )

        github_milestones_df = select_statement_executor.execute_query()

        return github_milestones_df

    def check_none(self, val):
        return "" if val is None else val

    def get_columns(self) -> List[str]:
        """Gets all columns to be returned in pandas DataFrame responses

        Returns
        -------
        List[str]
            List of columns
        """

        return [
            "url", 
            "html_url", 
            "labels_url", 
            "id", 
            "node_id", 
            "number", 
            "title", 
            "description", 
            "creator", 
            "open_issues", 
            "closed_issues", 
            "state", 
            "created_at", 
            "updated_at", 
            "due_on", 
            "closed_at"
        ]

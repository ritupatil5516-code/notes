# mongo_db_utils.py
import os
import json
from datetime import datetime

from pymongo import MongoClient, DESCENDING

# === Connection helpers ======================================================

def get_mongo_db():
    """
    Return a MongoDB Database object.

    Uses env vars:
      MONGO_URI      -> full URI  (eg: mongodb://localhost:27017/agent_desktop_release)
      or defaults to mongodb://localhost:27017/agent_desktop_release
    """
    default_uri = "mongodb://localhost:27017/agent_desktop_release"
    uri = os.getenv("MONGO_URI", default_uri)

    client = MongoClient(uri)
    # if DB name is in the URI, get_default_database() works; otherwise fall back
    db = client.get_default_database() or client["agent_desktop_release"]
    return db


# === RELEASE TAGS ============================================================

def insert_release_tag(db, project: dict, git_details: dict):
    """
    Upsert a release tag document in 'release_tags' collection using projectId as key.
    Mirrors the SQLite INSERT ... ON CONFLICT(projectId) DO UPDATE.
    """
    coll = db["release_tags"]
    now = datetime.utcnow()

    doc_fields = {
        "projectId":            project["projectId"],
        "projectName":          project["projectName"],
        "projectDisplayName":   project["projectDisplayName"],
        "projectType":          project["projectType"],
        "project_web_url":      git_details["project_web_url"],
        "tag_name":             git_details["tag_name"],
        "new_tag_status":       git_details["new_tag_status"],
        "new_tag_pipeline":     git_details["new_tag_pipeline"],
        "pat_uat_deployment":   git_details["pat_uat_deployment"],
        "current_deployed_tag_prod":          git_details["current_deployed_tag"],
        "current_deployed_tag_prod_pipeline": git_details["current_deployed_tag_pipeline"],
        "diff_url":            git_details["diff_url"],
        # in SQLite this was TEXT(JSON). In Mongo we'll keep it as list directly.
        "jira_issue_list":      git_details.get("jira_issue_list", []),
    }

    coll.update_one(
        {"projectId": project["projectId"]},
        {
            "$set": {**doc_fields, "updated_at": now},
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def update_release_tag_deployment_status(db, project_id: str, status: str, patuat_deploy_status: str):
    """
    UPDATE release_tags
       SET new_tag_status=?, pat_uat_deployment=?, updated_at=CURRENT_TIMESTAMP
     WHERE projectId=?
    """
    coll = db["release_tags"]
    now = datetime.utcnow()

    coll.update_one(
        {"projectId": project_id},
        {
            "$set": {
                "new_tag_status": status,
                "pat_uat_deployment": patuat_deploy_status,
                "updated_at": now,
            }
        },
    )


def update_release_tags(
    db,
    projectId: str,
    latest_tag: str,
    status: str,
    new_tag_pipeline: str,
    patuat_deploy_status: str,
    diff_url: str,
    jira_issue_list: list | None,
):
    """
    Mirrors the SQLite UPDATE that also merges JIRA issue lists.
    """
    coll = db["release_tags"]
    now = datetime.utcnow()

    # current Jira issues from DB
    existing = coll.find_one({"projectId": projectId}, {"jira_issue_list": 1})
    combined = set()

    if jira_issue_list:
        combined.update(jira_issue_list)

    if existing and existing.get("jira_issue_list"):
        combined.update(existing["jira_issue_list"])

    combined_list = list(combined)

    coll.update_one(
        {"projectId": projectId},
        {
            "$set": {
                "tag_name":           latest_tag,
                "new_tag_status":     status,
                "new_tag_pipeline":   new_tag_pipeline,
                "pat_uat_deployment": patuat_deploy_status,
                "diff_url":           diff_url,
                "jira_issue_list":    combined_list,
                "updated_at":         now,
            }
        },
    )


def get_release_tags(db) -> list[dict]:
    """
    SELECT * FROM release_tags ORDER BY created_at DESC
    """
    coll = db["release_tags"]
    docs = coll.find().sort("created_at", DESCENDING)

    results = []
    for d in docs:
        # keep fields similar to previous SQLite mapping
        results.append({
            "id":                        str(d.get("_id")),
            "projectId":                 d.get("projectId"),
            "projectName":               d.get("projectName"),
            "projectDisplayName":        d.get("projectDisplayName"),
            "projectType":               d.get("projectType"),
            "project_web_url":           d.get("project_web_url"),
            "tag_name":                  d.get("tag_name"),
            "new_tag_status":            d.get("new_tag_status"),
            "new_tag_pipeline":          d.get("new_tag_pipeline"),
            "pat_uat_deployment":        d.get("pat_uat_deployment"),
            "current_deployed_tag_prod": d.get("current_deployed_tag_prod"),
            "current_deployed_tag_prod_pipeline": d.get("current_deployed_tag_prod_pipeline"),
            "diff_url":                  d.get("diff_url"),
            "jira_issue_list":           d.get("jira_issue_list", []),
            "created_at":                d.get("created_at"),
            "updated_at":                d.get("updated_at"),
        })
    return results


def get_all_unique_jira_issues(db) -> list[str]:
    """
    SELECT DISTINCT jira_issue_list FROM release_tags;
    In SQLite this was JSON TEXT => flattened to unique list.
    Here they are stored as arrays already.
    """
    coll = db["release_tags"]
    unique = set()

    for d in coll.find({}, {"jira_issue_list": 1}):
        issues = d.get("jira_issue_list") or []
        unique.update(issues)

    return list(unique)


def get_all_unique_jira_issues_by_project(db, projectId: str) -> list[str]:
    """
    SELECT DISTINCT jira_issue_list FROM release_tags WHERE projectId=?;
    """
    coll = db["release_tags"]
    unique = set()

    for d in coll.find({"projectId": projectId}, {"jira_issue_list": 1}):
        issues = d.get("jira_issue_list") or []
        unique.update(issues)

    return list(unique)


# === CONFLUENCE INFO =========================================================

def insert_confluence_info(db, confluence_id: str, space_key: str, parent_page_id: str):
    """
    INSERT INTO confluence_info(page_id, space_key, parent_page_id)
    ON CONFLICT(page_id) ... (we just upsert by page_id).
    """
    coll = db["confluence_info"]
    now = datetime.utcnow()

    coll.update_one(
        {"page_id": confluence_id},
        {
            "$set": {
                "page_id":        confluence_id,
                "space_key":      space_key,
                "parent_page_id": parent_page_id,
                "updated_at":     now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )


def get_confluence_info(db) -> dict | None:
    """
    SELECT * FROM confluence_info ORDER BY created_at DESC LIMIT 1;
    """
    coll = db["confluence_info"]
    d = coll.find_one(sort=[("created_at", DESCENDING)])

    if not d:
        return None

    return {
        "id":            str(d.get("_id")),
        "page_id":       d.get("page_id"),
        "space_key":     d.get("space_key"),
        "parent_page_id": d.get("parent_page_id"),
        "created_at":    d.get("created_at"),
        "updated_at":    d.get("updated_at"),
    }


# === INTEGRATION TESTS =======================================================

def insert_integration_test_data(
    db,
    schedule_id: str,
    description: str,
    branch_ref: str,
    service_name: str,
    artifact_url: str,
):
    """
    INSERT INTO integration_tests(...) ON CONFLICT(schedule_id) DO UPDATE ...
    """
    coll = db["integration_tests"]
    now = datetime.utcnow()

    coll.update_one(
        {"schedule_id": schedule_id},
        {
            "$set": {
                "schedule_id":  schedule_id,
                "description":  description,
                "branch_ref":   branch_ref,
                "service_name": service_name,
                "artifact_url": artifact_url,
                "updated_at":   now,
            },
            "$setOnInsert": {"created_at": now},
        },
        upsert=True,
    )

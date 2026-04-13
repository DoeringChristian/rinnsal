"""initial metadata schema

Revision ID: 0001
Revises:
Create Date: 2026-04-13
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa


revision: str = "0001"
down_revision: str | None = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.create_table(
        "flows",
        sa.Column("name", sa.String(), primary_key=True),
        sa.Column("created_at", sa.Float(), nullable=False),
        sa.Column("last_run_at", sa.Float(), nullable=True),
    )
    op.create_table(
        "runs",
        sa.Column("run_id", sa.String(), primary_key=True),
        sa.Column(
            "flow_name",
            sa.String(),
            sa.ForeignKey("flows.name"),
            nullable=False,
        ),
        sa.Column("run_dir", sa.Text(), nullable=False),
        sa.Column(
            "status", sa.String(), nullable=False, server_default="running"
        ),
        sa.Column("started_at", sa.Float(), nullable=False),
        sa.Column("finished_at", sa.Float(), nullable=True),
        sa.Column("snapshot_hash", sa.String(), nullable=True),
        sa.Column("tags_json", sa.Text(), server_default="[]"),
        sa.Column("params_json", sa.Text(), server_default="{}"),
        sa.Column("task_count", sa.Integer(), server_default="0"),
        sa.Column("failed_count", sa.Integer(), server_default="0"),
    )
    op.create_index(
        "ix_runs_flow_started", "runs", ["flow_name", "started_at"]
    )
    op.create_index("ix_runs_flow_name", "runs", ["flow_name"])
    op.create_table(
        "task_nodes",
        sa.Column(
            "run_id",
            sa.String(),
            sa.ForeignKey("runs.run_id"),
            primary_key=True,
        ),
        sa.Column("task_name", sa.String(), primary_key=True),
        sa.Column("task_hash", sa.String(), server_default=""),
        sa.Column("status", sa.String(), server_default=""),
        sa.Column("duration", sa.Float(), server_default="0"),
        sa.Column("error", sa.Text(), server_default=""),
        sa.Column("ts", sa.Float(), server_default="0"),
        sa.Column("params_json", sa.Text(), server_default=""),
    )
    op.create_index("ix_task_nodes_name", "task_nodes", ["task_name"])
    op.create_table(
        "task_edges",
        sa.Column(
            "run_id",
            sa.String(),
            sa.ForeignKey("runs.run_id"),
            primary_key=True,
        ),
        sa.Column("from_task", sa.String(), primary_key=True),
        sa.Column("to_task", sa.String(), primary_key=True),
    )


def downgrade() -> None:
    op.drop_table("task_edges")
    op.drop_index("ix_task_nodes_name", table_name="task_nodes")
    op.drop_table("task_nodes")
    op.drop_index("ix_runs_flow_name", table_name="runs")
    op.drop_index("ix_runs_flow_started", table_name="runs")
    op.drop_table("runs")
    op.drop_table("flows")

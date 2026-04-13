"""SQLAlchemy declarative models for the metadata store schema."""

from __future__ import annotations

from sqlalchemy import (
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Flow(Base):
    __tablename__ = "flows"

    name: Mapped[str] = mapped_column(String, primary_key=True)
    created_at: Mapped[float] = mapped_column(Float, nullable=False)
    last_run_at: Mapped[float | None] = mapped_column(Float, nullable=True)


class Run(Base):
    __tablename__ = "runs"

    run_id: Mapped[str] = mapped_column(String, primary_key=True)
    flow_name: Mapped[str] = mapped_column(
        ForeignKey("flows.name"), nullable=False, index=True
    )
    run_dir: Mapped[str] = mapped_column(Text, nullable=False)
    status: Mapped[str] = mapped_column(
        String, nullable=False, default="running"
    )
    started_at: Mapped[float] = mapped_column(Float, nullable=False)
    finished_at: Mapped[float | None] = mapped_column(Float, nullable=True)
    snapshot_hash: Mapped[str | None] = mapped_column(String, nullable=True)
    tags_json: Mapped[str] = mapped_column(Text, default="[]")
    params_json: Mapped[str] = mapped_column(Text, default="{}")
    task_count: Mapped[int] = mapped_column(Integer, default=0)
    failed_count: Mapped[int] = mapped_column(Integer, default=0)

    __table_args__ = (
        Index("ix_runs_flow_started", "flow_name", "started_at"),
    )


class TaskNode(Base):
    __tablename__ = "task_nodes"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("runs.run_id"), primary_key=True
    )
    task_name: Mapped[str] = mapped_column(String, primary_key=True)
    task_hash: Mapped[str] = mapped_column(String, default="")
    status: Mapped[str] = mapped_column(String, default="")
    duration: Mapped[float] = mapped_column(Float, default=0.0)
    error: Mapped[str] = mapped_column(Text, default="")
    ts: Mapped[float] = mapped_column(Float, default=0.0)
    params_json: Mapped[str] = mapped_column(Text, default="")

    __table_args__ = (Index("ix_task_nodes_name", "task_name"),)


class TaskEdge(Base):
    __tablename__ = "task_edges"

    run_id: Mapped[str] = mapped_column(
        ForeignKey("runs.run_id"), primary_key=True
    )
    from_task: Mapped[str] = mapped_column(String, primary_key=True)
    to_task: Mapped[str] = mapped_column(String, primary_key=True)

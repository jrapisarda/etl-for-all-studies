"""SQLAlchemy models representing the star schema for the ETL pipeline."""
from __future__ import annotations

import datetime as dt
from typing import Optional

from sqlalchemy import DateTime, Float, ForeignKey, Index, Integer, String, UniqueConstraint
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    pass


class DimGene(Base):
    __tablename__ = "dim_gene"

    gene_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    ensembl_id: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)

    expressions: Mapped[list["FactExpression"]] = relationship(back_populates="gene")


class DimStudy(Base):
    __tablename__ = "dim_study"

    study_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gse_accession: Mapped[str] = mapped_column(String(32), unique=True, nullable=False)

    samples: Mapped[list["DimSample"]] = relationship(back_populates="study")
    expressions: Mapped[list["FactExpression"]] = relationship(back_populates="study")


class DimIllness(Base):
    __tablename__ = "dim_illness"

    illness_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    illness_label: Mapped[str] = mapped_column(String(128), unique=True, nullable=False)

    samples: Mapped[list["DimSample"]] = relationship(back_populates="illness")


class DimPlatform(Base):
    __tablename__ = "dim_platform"

    platform_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    platform_accession: Mapped[str] = mapped_column(String(64), unique=True, nullable=False)

    samples: Mapped[list["DimSample"]] = relationship(back_populates="platform")


class DimSample(Base):
    __tablename__ = "dim_sample"
    __table_args__ = (
        UniqueConstraint("gsm_accession", "study_key", name="uq_sample_per_study"),
    )

    sample_key: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    gsm_accession: Mapped[str] = mapped_column(String(32), nullable=False)
    study_key: Mapped[int] = mapped_column(ForeignKey("dim_study.study_key"), nullable=False)
    platform_key: Mapped[int | None] = mapped_column(ForeignKey("dim_platform.platform_key"))
    illness_key: Mapped[int | None] = mapped_column(ForeignKey("dim_illness.illness_key"))
    age: Mapped[str] = mapped_column(String(64), nullable=False, default="UNKNOWN")
    sex: Mapped[str] = mapped_column(String(32), nullable=False, default="UNKNOWN")

    study: Mapped[DimStudy] = relationship(back_populates="samples")
    platform: Mapped[DimPlatform | None] = relationship(back_populates="samples")
    illness: Mapped[DimIllness | None] = relationship(back_populates="samples")
    expressions: Mapped[list["FactExpression"]] = relationship(back_populates="sample")


class FactExpression(Base):
    __tablename__ = "fact_expression"
    __table_args__ = (
        UniqueConstraint("sample_key", "gene_key", "study_key", name="uq_expression_fact"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    sample_key: Mapped[int] = mapped_column(ForeignKey("dim_sample.sample_key"), nullable=False)
    gene_key: Mapped[int] = mapped_column(ForeignKey("dim_gene.gene_key"), nullable=False)
    study_key: Mapped[int] = mapped_column(ForeignKey("dim_study.study_key"), nullable=False)
    expression_value: Mapped[float] = mapped_column(Float, nullable=False)

    sample: Mapped[DimSample] = relationship(back_populates="expressions")
    gene: Mapped[DimGene] = relationship(back_populates="expressions")
    study: Mapped[DimStudy] = relationship(back_populates="expressions")


class FactGenePairCorrelation(Base):
    __tablename__ = "fact_gene_pair_corr"
    __table_args__ = (
        UniqueConstraint(
            "gene_a_key",
            "gene_b_key",
            "illness_key",
            "study_key",
            name="uq_gene_pair_corr",
        ),
        Index("ix_gene_pair_corr_gene_a", "gene_a_key"),
        Index("ix_gene_pair_corr_gene_b", "gene_b_key"),
        Index("ix_gene_pair_corr_illness", "illness_key"),
        Index("ix_gene_pair_corr_study", "study_key"),
    )

    correlation_key: Mapped[int] = mapped_column(
        Integer, primary_key=True, autoincrement=True
    )
    gene_a_key: Mapped[int] = mapped_column(ForeignKey("dim_gene.gene_key"), nullable=False)
    gene_b_key: Mapped[int] = mapped_column(ForeignKey("dim_gene.gene_key"), nullable=False)
    illness_key: Mapped[int] = mapped_column(ForeignKey("dim_illness.illness_key"), nullable=False)
    rho_spearman: Mapped[float] = mapped_column(Float, nullable=False)
    p_value: Mapped[float] = mapped_column(Float, nullable=False)
    q_value: Mapped[float | None] = mapped_column(Float)
    n_samples: Mapped[int] = mapped_column(Integer, nullable=False)
    computed_at: Mapped[str] = mapped_column(String(50), nullable=False)
    study_key: Mapped[int | None] = mapped_column(ForeignKey("dim_study.study_key"))

    gene_a: Mapped[DimGene] = relationship(foreign_keys=[gene_a_key])
    gene_b: Mapped[DimGene] = relationship(foreign_keys=[gene_b_key])
    illness: Mapped[DimIllness] = relationship()
    study: Mapped[DimStudy | None] = relationship()


class EtlStudyState(Base):
    __tablename__ = "etl_study_state"

    study_accession: Mapped[str] = mapped_column(String(32), primary_key=True)
    last_processed_gene: Mapped[str | None] = mapped_column(String(32))
    last_sample_index: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    metadata_loaded: Mapped[bool] = mapped_column(Integer, default=0, nullable=False)
    updated_at: Mapped[dt.datetime] = mapped_column(DateTime, default=dt.datetime.utcnow, nullable=False)


__all__ = [
    "Base",
    "DimGene",
    "DimSample",
    "DimStudy",
    "DimIllness",
    "DimPlatform",
    "FactExpression",
    "FactGenePairCorrelation",
    "EtlStudyState",
]

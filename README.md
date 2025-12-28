# TravelTide: Customer Segmentation Engine

![Python](https://img.shields.io/badge/Python-3.10-blue)
![Build](https://img.shields.io/badge/Build-Passing-green)
![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)

## 🎯 Business Context

TravelTide, a leading e-booking platform, faces a retention challenge despite having the largest inventory in the market. The goal of this project is to transition from generic marketing to **behavior-based personalization**.

This repository contains a production-grade **Machine Learning Pipeline** that segments users into 3 strategic cohorts to assign personalized perks (e.g., "Free Cancellation" for families).

## 🏗 Architecture & Engineering

Unlike a standard analysis notebook, this project is built as a modular Python package.

- **ETL:** Cohort filtering (Jan '22 - Dec '22) and outlier handling.
- **Feature Engineering:** Automated aggregation of clickstream & booking data.
- **Clustering:** K-Means algorithm with Scaler integration.
- **Quality Assurance:** Unit tests via `pytest` and modular `src` layout.

## 🚀 Quickstart

### Prerequisites

- Python 3.10+
- Poetry (recommended)

### Running the Pipeline

This project includes a **Mock Data Generator**. You can run it immediately without external datasets.

```bash
# 1. Install Dependencies
make install

# 2. Run Tests
make test

# 3. Execute Pipeline
make run
```


## 📊 Results (Example from Mock Run)

The pipeline identifies three distinct personas:

1. **Efficiency Travelers:** High booking volume, low clicks per session.
2. **Browsers / Cost Optimizers:** High clicks, high discount sensitivity.
3. **Flexibility Seekers:** High cancellation rate (Target for "Free Cancellation" perk).

## 🛠 Tech Stack

* **Core:** Python, Pandas, Scikit-Learn
* **DevOps:** Makefile, Poetry
* **Testing:** Pytest
* **CLI:** Typer


### Anleitung zum Starten

1. Erstelle die Dateien wie oben beschrieben.
2. Öffne dein Terminal im Ordner `traveltide-engine`.
3. Führe aus: `make install` (oder installiere die Libs manuell via pip, falls du kein Poetry hast).
4. Führe aus: `make run`.

**Ergebnis:** Du wirst sehen, wie die Pipeline durchläuft, synthetische Daten erstellt, Features baut, clustert und eine CSV ausspuckt. Das ist genau das, was du vorzeigen willst: **Funktionierende Software.**

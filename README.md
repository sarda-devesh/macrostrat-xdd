# macrostrat-xdd
Describe Macrostrat rock units using the scientific literature

This is a place to start compiling code and resources used to build better rock-record descriptions from the
geologic literature as part of the [**DARPA CriticalMAAS**](https://github.com/UW-Macrostrat/CriticalMAAS)
project.

## General research plan

- [Concepts linked to geologic units](notes/unit-description.md) can be used to help fill out a graph of units, their attributes, and names
- New units can be discovered by proximity to known entities

This research plan will be developed further during early 2024. The starting point is with two exploratory projects
taking different approaches to the problem.

- [`UW-Macrostrat/factsheet-generator`](https://github.com/UW-Macrostrat/factsheet-generator):
  An LLM-assisted generator for geological "fact sheets" that operates over the
  scientific literature (_Bill Xia_). This project implements "retrieval augmented generation"
  over pre-generated embeddings to find relevant text windows in a corpus of documents. The most
  relevant regions are then fed to a LLM for final fact synthesis.
- [`UW-Macrostrat/unsupervised-kg`](https://github.com/UW-Macrostrat/unsupervised-kg):
  Knowledge graph construction to discover new geologic entities in the
  scientific literature (_Devesh Sarda_). This system processes a corpus of documents
  to discover the relationshiops between system components, storing the result as a traversable graph. It is in early
  development, but seeks to illuminate the links and structured relationships between different geologic entities.

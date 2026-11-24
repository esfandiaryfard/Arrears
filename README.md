# Credit Card & Loan Defaulter Detection Pipeline (Apache Beam)

This project is a practice pipeline built with **Apache Beam** to identify banking customers who frequently miss or underpay their credit card and loan installments.

## Problem Overview

The pipeline evaluates customer payment behavior across two products and flags customers who default in **both** categories:

1. **Credit Card Defaulters:** Assigns points to customers based on their credit card payment behavior (such as missed, late, or partial payments) and sums the points to identify defaulters above a threshold.
2. **Loan Defaulters:** Assigns points for short or skipped loan payments, while **excluding** late payments tied to medical loans.
3. **Cross-Category Defaulters:** Joins or filters the results to find customers flagged as defaulters in **both** the credit card and loan categories.

## Pipeline Logic (High-Level)

- **Ingest:** Read customer credit card and loan payment records.
- **Score Credit Cards:** Map payment behavior to risk/default points and aggregate per customer.
- **Score Loans:** Filter out medical loan late-payment exceptions, assign points for short or skipped payments, and aggregate per customer.
- **Identify Overlap:** Filter both streams for defaulters and find customers present in both sets.

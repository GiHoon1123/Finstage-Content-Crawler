# Finstage Content Crawler

## Introduction

**Finstage Content Crawler** is a Python-based microservice that automatically collects and stores news articles related to publicly traded companies.  
It complements the core **Finstage** platform by enriching the system with relevant market content for frequently searched stock symbols.

Built as a modular, thread-safe, and event-driven pipeline, this crawler service demonstrates how to scale asynchronous data ingestion in a priority-based architecture using Kafka and multithreaded workers.

---

## Purpose

- Receive stock symbols asynchronously via Kafka from the main platform or related services.
- Classify symbols by importance using a scoring mechanism, and assign them to priority queues (TOP / MID / BOT).
- Extract article URLs using a BFS-based Google News crawler.
- Deduplicate content using both URL and hash-based checks.
- Store raw article content, metadata, and source domain in MySQL.
- Provide a RESTful API to retrieve saved content.

---

## Pipeline Overview

### Flow Summary

1. Symbols are consumed from a Kafka topic.
2. A priority classifier evaluates each symbol and places it into a TOP, MID, or BOT queue based on score.
3. The symbol router uses BFS to extract related news article URLs.
4. URLs are sent to priority-based URL queues (also TOP / MID / BOT).
5. A worker pool processes the URL queues concurrently, extracting content.
6. Duplicate URLs or identical content (based on SHA-256 hash) are filtered out.
7. Valid articles are saved in a relational database.

---

## Key Features

### Kafka-Based Symbol Ingestion

- Supports asynchronous symbol messages from any part of the Finstage ecosystem.

### Priority Queue Classification

- High-score symbols are crawled first, enabling real-time responsiveness for popular or volatile stocks.

### Google News URL Extraction (BFS)

- Searches through Google News results using a breadth-first approach to maximize URL coverage.

### Content Deduplication

- Uses both exact URL match and HTML hash comparison to filter duplicates.

### Multithreaded Worker Pool

- Distributes threads by queue priority (e.g., 5 for TOP, 3 for MID, 2 for BOT).

### RESTful API

- Fetches stored articles and metadata via paginated endpoints.

---

## REST API Endpoints

| Method | Endpoint             | Description                           |
| ------ | -------------------- | ------------------------------------- |
| GET    | `/contents`          | Paginated list of saved articles      |
| GET    | `/contents/{symbol}` | Retrieve a specific article by symbol |

> API documentation available at: `http://localhost:8082/docs`

---

## Why Kafka?

- Supports asynchronous and decoupled architecture
- Easy to scale consumers horizontally
- Naturally integrates with event-based features like search logs and user click streams
- Ideal for future real-time crawling pipelines

---

## Deduplication Logic

### URL Deduplication

- Checks if the article URL already exists in the database.

### Content Hash Deduplication

- Generates a SHA-256 hash from the article's HTML body.
- Prevents saving duplicated content with different URLs.

Only if **both checks pass**, the content is persisted.

---

## Tech Stack

- **Python 3.12**
- **FastAPI** – Asynchronous web framework
- **SQLAlchemy** – ORM for MySQL
- **MySQL 8.0** – Content storage
- **Kafka** – Event queue for symbol messages
- **Selenium** – Automated browser crawling
- **BeautifulSoup4** – HTML parsing
- **Thread-safe Queues** – Worker coordination
- **Uvicorn** – ASGI server

---

## Getting Started

### Prerequisites

- Python 3.12
- MySQL 8.x
- Kafka server running
- Chrome WebDriver installed

### Setup Instructions

```bash
# 1. Clone the repository
git clone https://github.com/your-username/finstage-content-crawler.git
cd finstage-content-crawler

# 2. Set up a virtual environment
python -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt

# 4. Run the FastAPI server
uvicorn app.main:app --host 0.0.0.0 --port 8082
```

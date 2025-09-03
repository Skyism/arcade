# Transactional Key-Value Store

A Python implementation of a transactional key-value store with support for nested transactions, persistence, and REST API access. Features a web-based GUI for testing and demonstration.

- Python 3.11 or higher
- pip (Python package installer)
- Git (for cloning the repository)

## Installation

### 1. Clone or Download the Project

```bash
# Clone the repository
git clone https://github.com/Skyism/arcade.git
cd arcade

# Or download and extract the project files to a directory
```

### 2. Create Virtual Environment

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
# On macOS/Linux:
source venv/bin/activate

# On Windows:
# venv\Scripts\activate
```

### 3. Install Dependencies

```bash
# Upgrade pip
pip install --upgrade pip

# Install project dependencies
pip install -r requirements.txt
```

### 4. Initialize Django Database

```bash
# Run Django migrations
python manage.py migrate
```

### 5. Verify Installation

```bash
# Run tests to verify everything works
python -m pytest tests/ -v

# Should show: ====== 97 passed in X.XXs ======
```


### Web GUI

1. **Start the Django server:**
```bash
python manage.py runserver 8000
```

2. **Open your web browser and navigate to:**
```
http://localhost:8000/api/gui/
```

3. **Follow the GUI workflow:**
   - Click "Check Health" to verify connection
   - Click "Initialize Store" to create session
   - Click "Begin Transaction" to start
   - Use the interface to set/get/delete keys
   - Click "Run Requirements Demo" to see the nested transaction example
   - Click "Commit Transaction" to save changes


## REST API Endpoints

### Store Management
- `GET /api/store/health/` - Health check
- `POST /api/store/init/` - Initialize store

### Transaction Management
- `POST /api/store/begin/` - Begin transaction
- `POST /api/store/commit/` - Commit transaction
- `POST /api/store/rollback/` - Rollback transaction
- `GET /api/store/transaction/status/` - Get transaction status

### Key-Value Operations
- `PUT /api/store/set/` - Set key-value pair
- `GET /api/store/get/{key}/` - Get value by key
- `DELETE /api/store/delete/{key}/` - Delete key

### Batch Operations
- `POST /api/store/batch/` - Execute multiple operations

### GUI Access
- `GET /api/gui/` - Web-based testing interface

## Project Structure

```
transactional-kv-store/
├── src/
│   └── kvstore/
│       ├── __init__.py
│       ├── store.py              # Main Store class
│       ├── async_store.py        # Async Store class
│       ├── transaction.py        # Transaction management
│       ├── async_transaction.py  # Async transaction management
│       ├── storage.py            # Storage backends
│       ├── async_storage.py      # Async storage backends
│       └── exceptions.py         # Custom exceptions
├── tests/
│   ├── test_store.py            # Core Store tests
│   ├── test_persistence.py      # Persistence tests
│   ├── test_api_comprehensive.py # API validation tests
│   ├── test_requirements_example.py # Requirements compliance
│   ├── test_integration.py      # Integration tests
│   ├── test_rest_api.py         # REST API tests
│   └── test_async_store.py      # Async implementation tests
├── kvstore_api/
│   ├── settings.py              # Django configuration
│   ├── urls.py                  # Main URL routing
│   └── wsgi.py                  # WSGI application
├── api/
│   ├── views.py                 # REST API views
│   ├── urls.py                  # API URL routing
│   ├── serializers.py           # Request/response serializers
│   └── store_manager.py         # Store session management
├── templates/
│   └── test_gui.html            # Web GUI interface
├── manage.py                    # Django management script
├── requirements.txt             # Python dependencies
└── README.md                    # This file
```

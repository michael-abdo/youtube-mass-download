#!/bin/bash

# Typing Clients Ingestion Pipeline - Setup Script
# Automates the initial setup process

set -e  # Exit on any error

echo "🚀 Setting up Typing Clients Ingestion Pipeline..."

# Check Python version
echo "📋 Checking Python version..."
python3 --version
if ! python3 -c "import sys; assert sys.version_info >= (3, 8)"; then
    echo "❌ Python 3.8+ required. Please upgrade Python."
    exit 1
fi
echo "✅ Python version OK"

# Install dependencies
echo "📦 Installing Python dependencies..."
if [ -f "requirements.txt" ]; then
    pip3 install -r requirements.txt
    echo "✅ Dependencies installed"
else
    echo "❌ requirements.txt not found!"
    exit 1
fi

# Setup configuration
echo "⚙️ Setting up configuration..."
if [ ! -f ".env" ]; then
    if [ -f ".env.example" ]; then
        cp .env.example .env
        echo "✅ Created .env from template"
        echo "📝 Please edit .env with your credentials"
    else
        echo "⚠️ .env.example not found, creating basic .env"
        cat > .env << 'EOF'
# Database Configuration
DB_PASSWORD=your_database_password_here

# AWS S3 Configuration (optional)
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
S3_BUCKET=

# Application Settings
DEBUG=false
LOG_LEVEL=INFO
EOF
        echo "✅ Created basic .env file"
    fi
else
    echo "✅ .env already exists"
fi

# Setup config.yaml
if [ ! -f "config/config.yaml" ]; then
    if [ -f "config/config.yaml.example" ]; then
        cp config/config.yaml.example config/config.yaml
        echo "✅ Created config.yaml from template"
    else
        echo "⚠️ config.yaml.example not found"
    fi
else
    echo "✅ config.yaml already exists"
fi

# Create necessary directories
echo "📁 Creating necessary directories..."
mkdir -p outputs
mkdir -p cache
mkdir -p logs
echo "✅ Directories created"

# Check ChromeDriver (optional)
echo "🌐 Checking ChromeDriver..."
if command -v chromedriver &> /dev/null; then
    echo "✅ ChromeDriver found: $(which chromedriver)"
elif command -v google-chrome &> /dev/null || command -v chromium &> /dev/null; then
    echo "⚠️ Chrome/Chromium found but ChromeDriver missing"
    echo "💡 Install with: brew install chromedriver (macOS) or download manually"
else
    echo "⚠️ Chrome/Chromium not found"
    echo "💡 Install Chrome or Chromium for web scraping features"
fi

# Test basic import
echo "🧪 Testing basic functionality..."
if python3 -c "
import sys
sys.path.append('.')
from utils.config import get_config
from utils.csv_manager import CSVManager
print('✅ Core imports successful')
"; then
    echo "✅ Basic functionality test passed"
else
    echo "❌ Basic functionality test failed"
    exit 1
fi

# Final instructions
echo ""
echo "🎉 Setup completed successfully!"
echo ""
echo "Next steps:"
echo "1. Edit .env with your credentials"
echo "2. Review config/config.yaml settings"
echo "3. Run a test: python3 simple_workflow.py --basic --test-limit 1"
echo ""
echo "For full documentation, see: README.md"
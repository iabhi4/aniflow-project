#!/bin/bash

# Check if Homebrew is installed
if ! command -v brew &> /dev/null; then
    echo "Homebrew not found. Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
fi

# Install Minikube
echo "Installing Minikube..."
brew install minikube

# Install kubectl
echo "Installing kubectl..."
brew install kubectl

# Install QEMU
echo "Installing QEMU..."
brew install qemu

# Install Helm
echo "Installing Helm..."
brew install helm

# Install Maven
echo "Installing Maven..."
brew install maven

# Enable NGINX Ingress Controller addon
echo "Enabling NGINX Ingress Controller addon..."
minikube addons enable ingress

# Enable metrics-server addon
echo "Enabling metrics-server addon..."
minikube addons enable metrics-server

# Add Bitnami Helm repository
echo "Adding Bitnami Helm repository..."
helm repo add bitnami https://charts.bitnami.com/bitnami

echo "Installation complete."
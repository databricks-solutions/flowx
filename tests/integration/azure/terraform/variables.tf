variable "subscription_id" {
  description = "Azure subscription targeted by the ephemeral test run."
  type        = string
  sensitive   = true
}

variable "tenant_id" {
  description = "Microsoft Entra tenant used for GitHub OIDC."
  type        = string
  sensitive   = true
}

variable "client_id" {
  description = "Client ID of the federated application."
  type        = string
  sensitive   = true
}

variable "owner_email" {
  description = "Owner email applied to every taggable integration resource."
  type        = string
  sensitive   = true

  validation {
    condition     = can(regex("^[^[:space:]@]+@[^[:space:]@]+\\.[^[:space:]@]+$", trimspace(var.owner_email)))
    error_message = "owner_email must be a non-empty email-like value."
  }
}

variable "location" {
  description = "Azure region for the integration resources."
  type        = string
  default     = "eastus2"
}

variable "name_prefix" {
  description = "Lowercase run-unique prefix; letters and numbers only. Names use its first 12 characters."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9]{3,32}$", var.name_prefix))
    error_message = "name_prefix must contain 3-32 lowercase letters or numbers."
  }
}

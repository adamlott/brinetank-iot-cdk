---
inclusion: always
---

# Coding Standards for Brine Tank IoT Project

## Python Style Guide

### General Principles
- Follow PEP 8 style guidelines
- Use type hints where appropriate
- Keep functions focused and single-purpose
- Prefer explicit over implicit

### Naming Conventions
- **Variables/Functions**: `snake_case`
- **Classes**: `PascalCase`
- **Constants**: `UPPER_SNAKE_CASE`
- **Private methods**: `_leading_underscore`

### Lambda Handler Structure
```python
import os, json, boto3, logging

# Initialize clients at module level (reused across invocations)
client = boto3.client("service")
log = logging.getLogger()
log.setLevel(logging.INFO)

# Environment variables
CONFIG_VAR = os.environ.get("CONFIG_VAR", "default")

def handler(event, context):
    """Lambda entry point with clear docstring."""
    # Parse event
    # Validate inputs
    # Process logic
    # Return structured response
    return {"ok": True, "data": result}
```

### Error Handling
- Use try-except blocks for external service calls
- Log errors with context: `log.error(f"Failed to process {device}: {e}")`
- Return meaningful error messages
- Don't let exceptions crash the Lambda silently

### DynamoDB Best Practices
- Use `Decimal` type for numeric values (not float)
- Include helper functions for type conversion
- Use batch operations when processing multiple items
- Leverage TTL for automatic data cleanup

### CDK Stack Patterns
- Keep stack definitions declarative
- Use environment variables for Lambda configuration
- Grant minimal IAM permissions
- Use `RemovalPolicy.RETAIN` for data tables
- Document resource naming conventions

## Testing Standards

### Unit Tests
- Test file naming: `test_*.py`
- Use pytest fixtures for common setup
- Mock AWS services using moto or boto3 stubber
- Test both success and failure paths

### Test Structure
```python
def test_calculate_fill_percentage():
    """Test description of what's being validated."""
    # Arrange
    distance = 40.0
    
    # Act
    result = calculate_fill_percentage(distance)
    
    # Assert
    assert result == expected_value
```

## AWS Resource Naming

### Convention
- Format: `{ResourceType}-{Environment}`
- Examples: `BrineTankIngest-prod`, `LowLevelAlert-dev`
- Use consistent environment names: `dev`, `staging`, `prod`

### DynamoDB Tables
- Use descriptive names: `BrineTankReadings`, `SensorNotificationConfig`
- Document partition and sort keys in comments
- Include purpose in CDK construct ID

## Environment Variables

### Lambda Configuration
- Always provide defaults: `os.getenv("VAR", "default")`
- Document expected values in code comments
- Use uppercase for environment variable names
- Group related configs together

### Sensitive Data
- Never hardcode credentials
- Use AWS Secrets Manager for secrets
- Reference IAM roles, not access keys
- Verify SES identities through AWS Console

## Git Commit Messages

### Format
```
<type>: <short summary>

<optional detailed description>
```

### Types
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `refactor`: Code restructuring
- `test`: Test additions/changes
- `chore`: Build/tooling changes

### Examples
- `feat: add temperature monitoring to ingest lambda`
- `fix: correct fill percentage calculation for edge cases`
- `docs: update README with SES configuration steps`

## Code Review Checklist

- [ ] Type hints added for function parameters
- [ ] Error handling for external service calls
- [ ] Logging statements for debugging
- [ ] Environment variables have defaults
- [ ] IAM permissions are minimal
- [ ] Tests cover new functionality
- [ ] Documentation updated
- [ ] No hardcoded values (emails, ARNs, etc.)
- [ ] DynamoDB uses Decimal for numbers
- [ ] Lambda timeout is appropriate

## Performance Considerations

### Lambda Optimization
- Initialize clients outside handler (cold start optimization)
- Use async invocation for non-blocking operations
- Set appropriate timeout values (10-30 seconds typical)
- Monitor CloudWatch metrics for optimization opportunities

### DynamoDB Optimization
- Use consistent reads only when necessary
- Leverage GSIs for alternate query patterns
- Batch operations when processing multiple items
- Monitor consumed capacity units

## Documentation Requirements

### Code Comments
- Explain "why" not "what"
- Document complex algorithms
- Include units for measurements (cm, %, seconds)
- Reference external resources when applicable

### Function Docstrings
```python
def calculate_fill_percentage(distance_cm: float) -> float:
    """
    Convert ultrasonic distance measurement to fill percentage.
    
    Args:
        distance_cm: Distance from sensor to salt surface in centimeters
        
    Returns:
        Fill percentage (0-100) where 100 is full tank
        
    Note:
        Uses EMPTY_DISTANCE and FULL_DISTANCE for calibration
    """
```

### README Updates
- Update README when adding features
- Include configuration examples
- Document new environment variables
- Add troubleshooting tips for common issues

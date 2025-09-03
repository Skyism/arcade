"""
Serializers for API requests and responses.
"""
from rest_framework import serializers


class SetKeySerializer(serializers.Serializer):
    """Serializer for set key operation."""
    key = serializers.CharField(max_length=255)
    value = serializers.JSONField()


class BatchOperationSerializer(serializers.Serializer):
    """Serializer for batch operations."""
    operations = serializers.ListField(
        child=serializers.DictField(),
        allow_empty=False
    )


class TransactionResponseSerializer(serializers.Serializer):
    """Serializer for transaction responses."""
    transaction_id = serializers.CharField()
    status = serializers.CharField()


class KeyValueResponseSerializer(serializers.Serializer):
    """Serializer for key-value responses."""
    key = serializers.CharField()
    value = serializers.JSONField()


class ErrorResponseSerializer(serializers.Serializer):
    """Serializer for error responses."""
    error = serializers.CharField()
    message = serializers.CharField()
    details = serializers.DictField(required=False)


class HealthCheckResponseSerializer(serializers.Serializer):
    """Serializer for health check responses."""
    status = serializers.CharField()
    timestamp = serializers.DateTimeField()
    version = serializers.CharField()
    store_status = serializers.CharField()


class BatchResponseSerializer(serializers.Serializer):
    """Serializer for batch operation responses."""
    results = serializers.ListField(
        child=serializers.DictField()
    )
    success_count = serializers.IntegerField()
    error_count = serializers.IntegerField()

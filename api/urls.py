"""
API URL configuration.
"""
from django.urls import path
from . import views

urlpatterns = [
    # Test GUI
    path('gui/', views.test_gui, name='test-gui'),
    
    # Store management
    path('store/init/', views.StoreInitView.as_view(), name='store-init'),
    path('store/health/', views.HealthCheckView.as_view(), name='health-check'),
    
    # Transaction management
    path('store/begin/', views.BeginTransactionView.as_view(), name='begin-transaction'),
    path('store/commit/', views.CommitTransactionView.as_view(), name='commit-transaction'),
    path('store/rollback/', views.RollbackTransactionView.as_view(), name='rollback-transaction'),
    path('store/transaction/status/', views.TransactionStatusView.as_view(), name='transaction-status'),
    
    # Key-value operations
    path('store/set/', views.SetKeyView.as_view(), name='set-key'),
    path('store/get/<str:key>/', views.GetKeyView.as_view(), name='get-key'),
    path('store/delete/<str:key>/', views.DeleteKeyView.as_view(), name='delete-key'),
    
    # Batch operations
    path('store/batch/', views.BatchOperationView.as_view(), name='batch-operations'),
]

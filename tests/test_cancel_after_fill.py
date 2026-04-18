from kafka_demo.services.logic import cancel_after_fill



def test_cancel_after_fill_edge_case_returns_special_event():
    assert cancel_after_fill(False) == "execution.order.cancel_after_fill"
    assert cancel_after_fill(True) == "execution.order.cancelled"

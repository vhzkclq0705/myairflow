import pytest
from myairflow.notify import send_noti

def test_notify():
    msg = "pytest: Jerry"
    status_code = send_noti(msg)
    assert status_code == 204
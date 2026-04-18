

def test_consumer_restart_from_committed_offset():
    committed_offset = 5
    messages = list(range(10))

    processed_before_restart = messages[: committed_offset + 1]
    processed_after_restart = messages[committed_offset + 1 :]

    assert processed_before_restart[-1] == committed_offset
    assert processed_after_restart[0] == committed_offset + 1
    assert len(processed_before_restart) + len(processed_after_restart) == len(messages)

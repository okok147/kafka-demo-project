from kafka_demo.services.logic import replay_rebuild_ids



def test_replay_rebuild_skips_done_ids():
    all_ids = [1, 2, 3, 4, 5]
    done = {2, 4}
    rebuilt = replay_rebuild_ids(all_ids, done)
    assert rebuilt == [1, 3, 5]

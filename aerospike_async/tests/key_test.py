from aerospike_async import Key


def test_key_eq():
    key1 = Key("test", "test", 1)
    key2 = Key("test", "test", 1)
    key3 = Key("test", "test", 2)
    key4 = Key("test1", "test", 1)
    key5 = Key("test", "test1", 1)

    # because key1 and key2 have matching set ("test") and user_key (1)
    assert key1 == key2

    # because key1's user_key (1) does not match key3's user_key (2)
    assert key1 != key3

    # because key1's and key4's set ("test") and user_key (1) match:
    assert key1 == key4

    # because key1's set ("test") does not match key5's set ("test1"):
    assert key1 != key5

def pad_byte_array(bytes, value=0):
    if len(bytes) != 4096:
        invalids = bytearray(4096 - len(bytes))
        for i in range(len(invalids)):
            invalids[i] = value
        bytes.extend(invalids)
    return bytes

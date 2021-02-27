def pad_byte_array(bytes, value=0):
    if len(bytes) != 4096:
        length = 4096 - len(bytes)
        invalids = bytearray(0)
        while len(invalids) != length:
            for num in int.to_bytes(value, length=8, byteorder="little"):
                invalids.append(num)
        bytes.extend(invalids)
    return bytes

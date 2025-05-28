import math

from bitstring import BitArray


def wgs84_to_hdlm_coord(lat, lng):
    lat_int_from_bin = math.floor(lat / (180 / math.pow(2, 31)))
    lng_int_from_bin = math.floor(lng / (360 / math.pow(2, 32)))
    b31_lat = '{:031b}'.format(lat_int_from_bin + (1 << 31))
    b32_lng = '{:032b}'.format(lng_int_from_bin)
    b64_lat_lng = '0{}0'.format(''.join(i + j for i, j in zip(b32_lng, b31_lat)))
    b64_bitarray = BitArray(bin=b64_lat_lng)
    b64_signed_integer = b64_bitarray.int
    return b64_signed_integer


def hdlm_coord_to_WGS84(b64_signed_integer):
    b64int = BitArray(int=b64_signed_integer, length=64)
    b64bin = b64int.bin
    print(b64bin)
    bin_index = 0
    b64bin_stripped = b64bin[1:]
    lng_bin = ''
    lat_bin = ''
    while bin_index < len(b64bin_stripped):
        if bin_index % 2 == 0:
            lng_bin += b64bin_stripped[bin_index]
        else:
            lat_bin += b64bin_stripped[bin_index]
        bin_index += 1
    lat_int_from_bin = BitArray(bin=lat_bin).int
    lng_int_from_bin = BitArray(bin=lng_bin).int
    lat_float_from_int = lat_int_from_bin * (180 / math.pow(2, 31))
    lng_float_from_int = lng_int_from_bin * (360 / math.pow(2, 32))
    return {'lat': lat_float_from_int, 'lng': lng_float_from_int}

# print(wgs84_to_hdlm_coord(52.283935546875, 12.908935546875))
# print(hdlm_coord_to_WGS84(604341051469070336))

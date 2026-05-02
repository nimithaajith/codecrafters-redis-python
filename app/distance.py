from math import radians, sin, cos, sqrt, asin


def haversine(lat1, lon1, lat2, lon2):
    R = 6372797.560856/1000 # Earth radius in kmeters

    dLat = radians(lat2 - lat1)
    dLon = radians(lon2 - lon1)
    lat1 = radians(lat1)
    lat2 = radians(lat2)

    a = sin(dLat / 2)**2 + cos(lat1) * cos(lat2) * sin(dLon / 2)**2
    c = 2 * asin(sqrt(a))

    return round(R * c * 1000 , 4) #in meters
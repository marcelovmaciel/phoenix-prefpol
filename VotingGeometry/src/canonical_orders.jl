const CANONICAL_3C_IDS = SVector{3,Int}[
    (@SVector [1, 2, 3]),  # ABC
    (@SVector [1, 3, 2]),  # ACB
    (@SVector [3, 1, 2]),  # CAB
    (@SVector [3, 2, 1]),  # CBA
    (@SVector [2, 3, 1]),  # BCA
    (@SVector [2, 1, 3]),  # BAC
]

const CANONICAL_4C_IDS = SVector{4,Int}[
    (@SVector [1, 2, 3, 4]),
    (@SVector [2, 1, 3, 4]),
    (@SVector [3, 1, 2, 4]),
    (@SVector [1, 3, 2, 4]),
    (@SVector [2, 3, 1, 4]),
    (@SVector [3, 2, 1, 4]),
    (@SVector [3, 2, 4, 1]),
    (@SVector [2, 3, 4, 1]),
    (@SVector [4, 3, 2, 1]),
    (@SVector [3, 4, 2, 1]),
    (@SVector [2, 4, 3, 1]),
    (@SVector [4, 2, 3, 1]),
    (@SVector [4, 1, 3, 2]),
    (@SVector [1, 4, 3, 2]),
    (@SVector [3, 4, 1, 2]),
    (@SVector [4, 3, 1, 2]),
    (@SVector [1, 3, 4, 2]),
    (@SVector [3, 1, 4, 2]),
    (@SVector [2, 1, 4, 3]),
    (@SVector [1, 2, 4, 3]),
    (@SVector [4, 2, 1, 3]),
    (@SVector [2, 4, 1, 3]),
    (@SVector [1, 4, 2, 3]),
    (@SVector [4, 1, 2, 3]),
]

const TETRAHEDRON_TEXT_POSITIONS = [
    (0.37, 0.10),
    (0.63, 0.10),
    (0.37, 0.53),
    (0.27, 0.27),
    (0.67, 0.27),
    (0.57, 0.53),
    (0.75, 0.60),
    (0.87, 0.33),
    (1.13, 0.77),
    (0.83, 0.77),
    (1.07, 0.33),
    (1.23, 0.60),
    (-0.25, 0.60),
    (-0.13, 0.33),
    (0.17, 0.77),
    (-0.17, 0.77),
    (0.07, 0.33),
    (0.25, 0.60),
    (0.63, -0.07),
    (0.37, -0.07),
    (0.57, -0.53),
    (0.77, -0.23),
    (0.23, -0.23),
    (0.37, -0.53),
]

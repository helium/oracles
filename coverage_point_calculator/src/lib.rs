#[derive(Debug)]
struct Radio;
#[derive(Debug)]
struct BoostedHexes;
#[derive(Debug)]
struct CoverageMap;

#[derive(Debug, PartialEq)]
struct CoveragePoints;

fn calculate(
    _radio: Radio,
    _boosted_hexes: BoostedHexes,
    _coverage_map: CoverageMap,
) -> CoveragePoints {
    CoveragePoints
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let radio = Radio;
        let boosted_hexes = BoostedHexes;
        let coverage_map = CoverageMap;

        let coverage_points = calculate(radio, boosted_hexes, coverage_map);

        assert_eq!(coverage_points, CoveragePoints);
    }
}

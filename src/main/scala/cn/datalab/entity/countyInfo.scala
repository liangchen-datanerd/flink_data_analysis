package cn.datalab.entity

/**
 *
 * @param county
 * @param state
 * @param FIPS
 * @param population
 * @param area
 * @param density
 */
case class countyInfo(
                       county: String,
                       state: String,
                       FIPS: String,
                       population: Int,
                       area: Int,
                       density: Int
                     )

collect_monitorar_saude <- function() {
  # Connect to database (read only)
  con <- DBI::dbConnect(
    duckdb::duckdb(),
    "../camsdata/forecast_data/cams_forecast.duckdb",
    read_only = TRUE
  )

  # Tables
  tb_pm25 <- "pm25_mun_forecast"

  # Query
  res <- dplyr::tbl(con, tb_pm25) |>
    dplyr::group_by(code_muni) |>
    dplyr::arrange(date) |>
    dplyr::filter(value >= 15) |>
    dplyr::ungroup() |>
    dplyr::arrange(code_muni, date) |>
    dplyr::collect() |>
    dplyr::mutate(date = lubridate::date(date)) |>
    dplyr::select(code_muni, date) |>
    dplyr::distinct()

  message_df <- res |>
    dplyr::group_by(code_muni) |>
    dplyr::summarise(
      dates = list(as.character(format(date, "%d/%m/%Y (%A)")))
    ) |>
    dplyr::ungroup() |>
    dplyr::group_by(code_muni) |>
    dplyr::mutate(dates = paste(unlist(dates), collapse = ", ")) |>
    dplyr::mutate(
      code_muni = as.character(code_muni),
      identifier = uuid::UUIDgenerate(),
      title = "Alerta de PM2.5",
      message = glue::glue(
        "Estima-se que nos próximos sete dias o município apresente concentrações de PM 2.5 acima dos valores recomendados pela Organização Mundial da Saúde (OMS) nas seguinte(s) data(s): {dates}."
      )
    ) |>
    dplyr::select(identifier, code_muni, title, message)

  DBI::dbDisconnect(con)

  return(message_df)
}

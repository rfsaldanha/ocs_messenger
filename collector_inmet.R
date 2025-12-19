collect_inmet <- function(last_n) {
  # INMET parquet file address
  parquet_url <- "https://inmetalerts.nyc3.digitaloceanspaces.com/inmetalerts.parquet"

  # Read data
  res <- arrow::read_parquet(parquet_url) |>
    dplyr::distinct()

  if (nrow(res) == 0) {
    return(NULL)
  }

  # Parse inmet data
  res <- inmetrss::parse_mun(res, text = TRUE)

  # Filter last n entries per municipality
  res <- res |>
    dplyr::group_by(mun_codes) |>
    dplyr::arrange(dplyr::desc(sent)) |>
    dplyr::slice_head(n = last_n) |>
    dplyr::ungroup()

  # Prepare message data
  message_df <- res |>
    dplyr::mutate(title = "Alerta INMET") |>
    dplyr::mutate(message = paste(description, instruction)) |>
    dplyr::select(
      identifier,
      code_muni = mun_codes,
      title,
      message
    )

  return(message_df)
}

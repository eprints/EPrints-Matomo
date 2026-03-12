# EPrints-Matomo

*Alternative EPrints extension for sending usage pings to the Matomo tracking API (OpenAIRE uses Matomo)*

This is a fork and major refactor of the work by Alex Ball: https://github.com/alex-ballEPrints-OAPing/

Changes from EPrints-OAPing:
 - Only bulk uploads, no live pings
 - Explicit support for any Matomo instance, not just OpenAIRE
 - Minimal setup required to upload historic access data
 - Improved detection of errors from Matomo

## Installation

You can install this as an ingredient that you can then load into archives on a flavour-by-flavour basis;

Check out the git repository into into your `~eprints/ingredients` folder, where
`~eprints` is typically something like `/opt/eprints3`.

For EPrints 3.4, edit `flavours/pub_lib/inc` and add the line `ingredients/EPrints-Matomo`

## Configuration

To configure the ingredient for your archive, copy `ingredients/EPrints-Matomo/cfg.d/z_matomo_config.pl.example` to `archives/[YOUR_ARCHIVE_ID]/cfg/cfg.d/z_matomo_config.pl`.

You will want to configure at least `$c->{matomo}->{idsite}` and `$c->{matomo}->{token_auth}`.

Remember to restart both the server and Indexer after changing the
configuration.

## Inital Setup

To start uploading historic access data, run `./bin/start_historic_access_upload.pl ARCHIVE_ID`.

Add the following to crontab to trigger daily upload at 3am:

`* 3 * * * perl /opt/eprint3s/ingredients/EPrints-Matomo/bin/yesterdays_accesses.pl ARCHIVE_ID`

Note that this script (and all other scripts in this ingredient) uses the UTC timezone.

## Operation

The matomo plugin works hard to ensure all pings get through to the tracker safely. Unsuccessful pings are retried until they succeed. If individual access requests in a batch are rejected by Matomo, but the request to process the batch is successful, they will be skipped to prevent getting stuck.

All bulk requests are transmitted in batches of configurable size (`$c->{matomo}->{max_payload}`, default 1000).


## Debugging

The plugin writes a log file for each batch being transmitted:

-   **ARCHIVE_ID/var/matomo/*.json**

    This records information about the last run of a batch job. It is also used to keep track of how far through the batch notifications it has progressed. Batches are named `legacy_access` for the initial upload of historic data and `daily_YYYY-MM-DD` for the daily uploads.


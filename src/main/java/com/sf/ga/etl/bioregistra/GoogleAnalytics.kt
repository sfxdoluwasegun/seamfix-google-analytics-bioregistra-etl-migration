package com.sf.ga.etl.bioregistra

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes
import com.google.api.services.analyticsreporting.v4.model.DateRange
import com.google.api.services.analyticsreporting.v4.model.GetReportsRequest
import com.google.api.services.analyticsreporting.v4.model.Metric
import com.google.api.services.analyticsreporting.v4.model.ReportRequest
import org.json.JSONObject
import java.io.*
import java.lang.IllegalStateException
import java.security.GeneralSecurityException
import com.google.api.services.analyticsreporting.v4.model.DateRangeValues
import com.google.api.services.analyticsreporting.v4.model.ReportRow
import com.google.api.services.analyticsreporting.v4.model.MetricHeaderEntry
import com.google.api.services.analyticsreporting.v4.model.ColumnHeader
import com.google.api.services.analyticsreporting.v4.model.Report
import com.google.api.services.analyticsreporting.v4.model.GetReportsResponse



/**
 * This class handles all Google Analytics reporting operations
 * @author Biose, Nonso Emmanuel
 * @since 2019-07-08
 */
class GoogleAnalytics {

    companion object {
        private const val GA_PREFIX = "ga:"
        private const val APPLICATION_NAME = "Google Analytics ETL BioRegistra"
        private const val GOOGLE_ANALYTICS_VIEW_ID = "150721365"
    }

    private lateinit var query: JSONObject

    /**
     * This method loads the Query json file and returns the Json String
     * @param path to the Query Json file
     * @return JSONObject containing the JSON
     */
    private fun loadJsonQuery(path: String): JSONObject{
        val file = File(path)
        if(file.exists()) {
            println("Loading: ${file.name} from ${file.parent}")
            return FileReader(file).use {reader ->
                JSONObject(reader.readText())
            }
        }
        println("Path: ${file.absolutePath} doesn't exist")
        return JSONObject("{}")
    }

    /**
     * This method loads the json file containing all credentials needed for consuming the Google Analytics Reports
     * @param path to the credentials file
     * @return InputStream
     */
    private fun loadGoogleAnalyticsCredentials(path: String): InputStream? {
        val file = File(path)
        if (file.exists())  {
            println("Loading: ${file.name} from ${file.parent}")
            return FileInputStream(file)
        }
        println("Path: ${file.absolutePath} doesn't exist")
        return null
    }

    /**
     * This method initialises the Analytics reporting tool used to query the google analytics
     * @param googleAnalyticsCredentialPath to the Google Analytics Credentials
     * @param googleAnalyticsQueryPath to the Google Analytics Query
     * @return AnalyticsReporting
     */
    @Throws(GeneralSecurityException::class, IOException::class)
    fun initialiseAnalytics(googleAnalyticsCredentialPath: String, googleAnalyticsQueryPath: String): AnalyticsReporting {

        println("Initialising Google Analytics . . .")

        query = loadJsonQuery(googleAnalyticsQueryPath)

        val httpTransport = GoogleNetHttpTransport.newTrustedTransport() as HttpTransport
        val jsonFactory = JacksonFactory.getDefaultInstance()

        val inputStream = loadGoogleAnalyticsCredentials(googleAnalyticsCredentialPath)
        inputStream?.let {
            val googleCredential = GoogleCredential.fromStream(inputStream).createScoped(AnalyticsReportingScopes.all())
            return AnalyticsReporting.Builder(httpTransport, jsonFactory, googleCredential).setApplicationName(
                APPLICATION_NAME
            )
                .build()
        }  ?: throw IllegalStateException("Unable to initialise Analytics Reporting tool")
    }

    /**
     * This method, builds the Google Analytics query
     * @return GetReportsRequest containing the query
     */
    fun buildQuery(): GetReportsRequest {

        // Create date range
        val dateRange = DateRange()
        dateRange.startDate = query.getString(QueryConstants.START_DATE)
        dateRange.endDate = query.getString(QueryConstants.END_DATE)

        println("Setting Date range from: ${dateRange.startDate} to ${dateRange.endDate}")

        // Create metrics
        val metric = Metric().apply {
            val queries = query.getJSONArray(QueryConstants.QUERIES)
            val query= queries.getJSONObject(0)
            val metrics = query.getJSONArray(QueryConstants.QUERY_METRICS)
            metrics.forEach { println("Setting metrics: $it\n") }
            val metric = metrics.getString(0)
            this.expression = GA_PREFIX.plus(metric)
            this.alias = metric
        }

        println("Creating Google Analytics Report Request")

        val reportRequest = ReportRequest().apply {
            this.viewId = GA_PREFIX.plus(GOOGLE_ANALYTICS_VIEW_ID)
            this.dateRanges = arrayListOf(dateRange)
            this.metrics = arrayListOf(metric)

        }

        return GetReportsRequest().apply {
            this.reportRequests = arrayListOf(reportRequest)
        }

    }

    /**
     * This method makes a request to Google Analytics
     * @return GetReportsResponse containing the responses
     */
    fun makeRequest(request: GetReportsRequest, analyticsReporter: AnalyticsReporting): GetReportsResponse {
        println("Making request to Google Analytics with View ID: $GOOGLE_ANALYTICS_VIEW_ID")
        return analyticsReporter.reports().batchGet(request).execute()
    }

    /**
     * Parses and prints the Analytics Reporting API V4 response.
     *
     * @param response An Analytics Reporting API V4 response.
     */
    fun printResponse(response: GetReportsResponse) {

        for (report in response.reports) {
            val header = report.columnHeader
            val dimensionHeaders = header.dimensions
            val metricHeaders = header.metricHeader.metricHeaderEntries
            val rows = report.data.rows

            if (rows == null) {
                System.out.println("No data found for $GOOGLE_ANALYTICS_VIEW_ID")
                return
            }

            for (row in rows) {
                val dimensions = row.dimensions
                val metrics = row.metrics

                var i = 0
//                while (i < dimensionHeaders.size && i < dimensions.size) {
//                    println(dimensionHeaders[i] + ": " + dimensions[i])
//                    i++
//                }

                for (j in metrics.indices) {
                    print("Date Range ($j): ")
                    val values = metrics[j]
                    var k = 0
                    while (k < values.getValues().size && k < metricHeaders.size) {
                        println(metricHeaders[k].name + ": " + values.getValues()[k])
                        k++
                    }
                }
            }
        }
    }

}
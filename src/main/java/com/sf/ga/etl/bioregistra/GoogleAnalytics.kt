package com.sf.ga.etl.bioregistra

import com.google.api.client.googleapis.auth.oauth2.GoogleCredential
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport
import com.google.api.client.http.HttpTransport
import com.google.api.client.json.jackson2.JacksonFactory
import com.google.api.services.analyticsreporting.v4.AnalyticsReporting
import com.google.api.services.analyticsreporting.v4.AnalyticsReportingScopes
import com.google.api.services.analyticsreporting.v4.model.*
import org.json.JSONObject
import java.io.*
import java.lang.IllegalStateException
import java.net.UnknownHostException
import java.security.GeneralSecurityException
import java.text.SimpleDateFormat
import java.util.*
import kotlin.collections.ArrayList


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
        private const val CSV_PREFIX = "GA-BIOREGISTRA-"
    }

    private lateinit var query: JSONObject

    /**
     * This method loads the Query json file and returns the Json String
     * @param path to the Query Json file
     * @return JSONObject containing the JSON
     */
    private fun loadJsonQuery(path: String): JSONObject {
        val file = File(path)
        if (file.exists()) {
            println("Loading: ${file.name} from ${file.parent}")
            return FileReader(file).use { reader ->
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
        if (file.exists()) {
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
    fun initialiseAnalytics(
        googleAnalyticsCredentialPath: String,
        googleAnalyticsQueryPath: String
    ): AnalyticsReporting {

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
        } ?: throw IllegalStateException("Unable to initialise Analytics Reporting tool")
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
        val queries = query.getJSONArray(QueryConstants.QUERIES)
        val query = queries.getJSONObject(0)
        val metrics = query.getJSONArray(QueryConstants.QUERY_METRICS)
        val listOfMetrics = ArrayList<Metric>()
        metrics.forEachIndexed { _, expression ->
            val metric = Metric().apply {
                this.expression = GA_PREFIX.plus(expression)
                this.alias = expression as String
                println("Setting metrics: ${this.expression}\n")
            }
            listOfMetrics.add(metric)
        }

        // Create Dimension
        val dimensions = query.getJSONArray(QueryConstants.QUERY_METRIC_DIMENSIONS).toList()
        val otherDimensions = query.getJSONArray(QueryConstants.QUERY_OTHER_DIMENSIONS).toList()
        val allDimensions = arrayListOf(*dimensions.toTypedArray(), *otherDimensions.toTypedArray())
        val listOfDimensions = ArrayList<Dimension>()
        dimensions.forEachIndexed { _, expression ->
            val dimension = Dimension().apply {
                this.name = GA_PREFIX.plus(expression)
                println("Applying dimensions: ${this.name}\n")
            }
            listOfDimensions.add(dimension)
        }

        println("Creating Google Analytics Report Request")

        val reportRequest = ReportRequest().apply {
            this.viewId = GA_PREFIX.plus(GOOGLE_ANALYTICS_VIEW_ID)
            this.dateRanges = arrayListOf(dateRange)
            this.metrics = listOfMetrics
            this.dimensions = listOfDimensions

        }

        return GetReportsRequest().apply {
            this.reportRequests = arrayListOf(reportRequest)
        }

    }

    /**
     * This method makes a request to Google Analytics
     * @param request containing the query
     * @param analyticsReporter used to make the reports
     * @return GetReportsResponse containing the responses
     */
    @Throws(UnknownHostException::class)
    fun makeRequest(request: GetReportsRequest, analyticsReporter: AnalyticsReporting): GetReportsResponse {
        println("Making request to Google Analytics with View ID: $GOOGLE_ANALYTICS_VIEW_ID")
        return analyticsReporter.reports().batchGet(request).execute()
    }

    /**
     * Parses and prints the Analytics Reporting API V4 response.
     *
     * @param response An Analytics Reporting API V4 response.
     */
    fun generateCSV(response: GetReportsResponse, destinationPath: String) {

        val destinationDir = File(destinationPath)

        if (destinationDir.exists()) {

            val csv = File(destinationDir, "$CSV_PREFIX${generateDate()}.csv")
            val bufferedWriter = BufferedWriter(FileWriter(csv))

            println("Generating ${csv.name}")

            for (report in response.reports) {
                val header = report.columnHeader
                val dimensionHeaders = header.dimensions
                val metricHeadersEntries = header.metricHeader.metricHeaderEntries
                val rows = report.data.rows

                if (rows.isEmpty()) {
                    println("No data found for $GOOGLE_ANALYTICS_VIEW_ID")
                    return
                }

                bufferedWriter.use { writer ->

                    val metricHeaders = metricHeadersEntries.map { it.name }
                    val headers = dimensionHeaders.joinToString(", ") { it.split(":")[1] }.plus(", ")
                        .plus(metricHeaders.joinToString(", "))

                    writer.write(headers)
                    writer.newLine()
                    writer.flush()

                    for (row in rows) {
                        val dimensions = row.dimensions
                        val metricValues = row.metrics
                        var dimensionsCounter = 0

                        val dimensionsAndMetricsData = ArrayList<String>()

                        while (dimensionsCounter < dimensionHeaders.size && dimensionsCounter < dimensions.size) {
                            val dimension = dimensions[dimensionsCounter]
                            dimensionsAndMetricsData.add(dimension)
                            dimensionsCounter++
                        }

                        for (metricIndex in metricValues.indices) {
                            val metricValue = metricValues[metricIndex]
                            var metricCounter = 0
                            while (metricCounter < metricHeadersEntries.size && metricCounter < metricValue.getValues().size) {
                                val metric = metricValue.getValues()[metricCounter]
                                dimensionsAndMetricsData.add(metric)
                                metricCounter++
                            }
                        }
                        val line = dimensionsAndMetricsData.joinToString(", ")
                        writer.write(line)
                        writer.newLine()
                        writer.flush()
                    }

                }
            }

            println("${csv.name} can be found at ${csv.parent}")
        } else {
            println("Path: ${destinationDir.absolutePath} doesn't exist")
        }
    }

    /**
     * This function is used to generate a date in a readable format
     * @return date in readable format
     */
    private fun generateDate(): String {
        val date = Date()
        val simpleDateFormat = SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        return simpleDateFormat.format(date)
    }
}
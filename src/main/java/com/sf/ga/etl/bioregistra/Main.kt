package com.sf.ga.etl.bioregistra

import java.net.SocketException
import java.net.UnknownHostException


fun main() {
    val googleAnalytics = GoogleAnalytics()
    val reporter = googleAnalytics.initialiseAnalytics("/Users/nonsobiose/Downloads/ga-bioregistra-etl-fbc2fa24d6cb.json", "/Users/nonsobiose/IdeaProjects/Google Analytics ETL Bioregistra/Query.json")
    val queries = googleAnalytics.buildQuery()
    try {
        queries.forEach {queryEntry ->
            val response = googleAnalytics.makeRequest(queryEntry.value, reporter)
            googleAnalytics.generateCSV(response, "/Users/nonsobiose/Downloads/", queryEntry.key)
        }

    } catch (ex: UnknownHostException) {
        println(ex.message)
    } catch (ex: SocketException) {
        println(ex.message)
    }
}

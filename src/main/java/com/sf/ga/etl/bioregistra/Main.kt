package com.sf.ga.etl.bioregistra


fun main() {
    val googleAnalytics = GoogleAnalytics()
    val reporter = googleAnalytics.initialiseAnalytics("/Users/nonsobiose/Downloads/ga-bioregistra-etl-fbc2fa24d6cb.json", "/Users/nonsobiose/IdeaProjects/Google Analytics ETL Bioregistra/Query.json")
    val query = googleAnalytics.buildQuery()
    val response = googleAnalytics.makeRequest(query, reporter)
    googleAnalytics.printResponse(response)
}

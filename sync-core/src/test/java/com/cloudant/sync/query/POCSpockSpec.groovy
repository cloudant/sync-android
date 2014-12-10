package com.cloudant.sync.query

import spock.lang.Specification

class POCSpockSpec extends Specification {

    def calc = new POCCalculation()

    def "addition of two numbers"() {
        given: "two numbers"
        def num1
        def num2
        when: "the numbers are 1 and 2"
        num1 = 1
        num2 = 2
        then: "the result is 3"
        calc.add(num1, num2) == 3
        when: "the numbers are 2 and 3"
        num1 = 2
        num2 = 3
        then: "the result is 5"
        calc.add(num1, num2)
    }

    def "subtraction of two numbers"() {
        given: "two numbers"
        def num1
        def num2
        when: "the numbers are 2 and 1"
        num1 = 2
        num2 = 1
        then: "the result is 1"
        calc.subtract(num1, num2) == 1
        when: "the numbers are 5 and 3"
        num1 = 5
        num2 = 3
        then: "the result is 2"
        calc.subtract(num1, num2)
    }
}
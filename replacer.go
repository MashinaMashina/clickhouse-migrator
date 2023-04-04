package main

import (
	"fmt"
	"log"
	"regexp"
	"strings"
)

var (
	isDistributed        = regexp.MustCompile(`(?is)engine = distributed\(.+?,.+?,(.+?),.+?\)\s*;`)
	tableOrder           = regexp.MustCompile(`(?is)create table (?:if not exists |)(.+?)\(.+?order by\s*\(([^\)]+?)\)`)
	commentWithDelimiter = regexp.MustCompile(`--.*;.*`)
)

func replaceText(str string) string {
	badComments := commentWithDelimiter.FindAllString(str, -1)
	for _, comment := range badComments {
		log.Printf("maybe multi statement delimiter (;) in comment near '%s'", prepareBadCommentMessage(comment))
	}

	str = strings.NewReplacer(
		"ON CLUSTER '{cluster}'", "",
		"on cluster '{cluster}'", "",
		"defaultdb.", "",
	).Replace(str)

	if distMatch := isDistributed.FindAllStringSubmatch(str, -1); len(distMatch) > 0 {
		ordersMatch := tableOrder.FindAllStringSubmatch(str, -1)

		orders := make(map[string]string, len(ordersMatch))
		for _, order := range ordersMatch {
			orders[strings.TrimSpace(order[1])] = order[2]
		}

		replace := make([]string, 0, len(distMatch)*2)
		for _, dist := range distMatch {
			table := strings.Trim(dist[1], "\r\n\t '\"")
			if order, ok := orders[table]; ok {
				replace = append(replace,
					dist[0], fmt.Sprintf("engine = MergeTree order by (%s);", order),
				)
			}
		}

		str = strings.NewReplacer(replace...).Replace(str)
	}

	return str
}

func prepareBadCommentMessage(comment string) string {
	comment = strings.TrimSpace(comment)

	if len(comment) > 100 {
		comment = comment[0:45] + " ... " + comment[len(comment)-45:]
	}

	return comment
}

package utils

import (
	"regexp"
	"strings"
)

func MatchPatternKeys(keys []string, pattr string) ([]string, error) {
	res := make([]string, 0)

	// check made if a valid regex Unicode point is present
	if strings.ContainsAny(pattr, "*?[]^") {
		re, err := regexp.Compile(pattr)
		if err != nil {
			return res, err
		}
		for _, key := range keys {
			if re.MatchString(key) {
				res = append(res, key)
			}
		}
	} else {
		// if valid regex unicode point doesn't match use direct match
		for _, key := range keys {
			if key == pattr {
				res = append(res, key)
			}
		}
	}

	return res, nil
}

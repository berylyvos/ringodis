package database

func readFirstKey(args CmdArgs) ([]string, []string) {
	key := string(args[0])
	return nil, []string{key}
}

func writeFirstKey(args CmdArgs) ([]string, []string) {
	key := string(args[0])
	return []string{key}, nil
}

func readAllKeys(args CmdArgs) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return nil, keys
}

func writeAllKeys(args CmdArgs) ([]string, []string) {
	keys := make([]string, len(args))
	for i, v := range args {
		keys[i] = string(v)
	}
	return keys, nil
}

func noPrepare(args CmdArgs) ([]string, []string) {
	return nil, nil
}
package generator_test

import "github.com/accented-ai/pgtofu/internal/generator"

func testOptions() *generator.Options {
	opts := generator.DefaultOptions()
	opts.PreviewMode = true

	return opts
}

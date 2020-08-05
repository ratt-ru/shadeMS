## Verify `shamems` top level functionality using integration tests

- View options available    
`./check-shadems-functionality.sh -h`

- Basic functionality tests that all examples in the top level README succeeds    
`./check-shadems-functionality.sh msfiles/small.ms`    
and clear old output    
`./check-shadems-functionality.sh msfiles/small.ms -c`

- Extended tests that have argument permutations for development    
`./check-shadems-functionality.sh msfiles/small.ms -c -x`

- Inducing parser errors to see that option restrictions will stop before functionality   
`./check-shadems-functionality.sh msfiles/small.ms -c -e`

- Run all    
`./check-shadems-functionality.sh msfiles/small.ms -c -e -x`


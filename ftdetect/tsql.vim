au BufRead,BufNewFile *.sql set filetype=tsql | syntax sync fromstart
au BufEnter * if &filetype ==# 'tsql' | syntax sync fromstart | endif

extern crate proc_macro;

use proc_macro::TokenStream;
use quote::quote;
use syn::{
    parse::{Parse, ParseStream},
    parse_macro_input, Attribute, ItemFn,
};

#[derive(Debug)]
struct WorkerOpts {
    concurrency: u32,
    retry: u32,
}

struct WorkerOptsBuilder {
    concurrency: Option<u32>,
    retry: Option<u32>,
}

impl WorkerOptsBuilder {
    fn new() -> Self {
        WorkerOptsBuilder {
            concurrency: None,
            retry: None,
        }
    }

    fn concurrency(mut self, concurrency: u32) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    fn retry(mut self, retry: u32) -> Self {
        self.retry = Some(retry);
        self
    }

    fn build(self) -> WorkerOpts {
        WorkerOpts {
            concurrency: self.concurrency.unwrap_or(1),
            retry: self.retry.unwrap_or(0),
        }
    }
}

impl Parse for WorkerOpts {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut opts = WorkerOptsBuilder::new();

        while !input.is_empty() {
            let lookahead = input.lookahead1();
            if lookahead.peek(syn::Ident) {
                let ident: syn::Ident = input.parse()?;
                match ident.to_string().as_str() {
                    "concurrency" => {
                        input.parse::<syn::Token![=]>()?;
                        let concurrency: syn::LitInt = input.parse()?;
                        opts = opts.concurrency(concurrency.base10_parse()?);
                    }
                    "retry" => {
                        input.parse::<syn::Token![=]>()?;
                        let retry: syn::LitInt = input.parse()?;
                        opts = opts.retry(retry.base10_parse()?);
                    }
                    _ => {
                        return Err(syn::Error::new(
                            ident.span(),
                            format!("unexpected option: {}", ident),
                        ))
                    }
                }

                if !input.is_empty() {
                    input.parse::<syn::Token![,]>()?;
                }
            } else {
                return Err(lookahead.error());
            }
        }

        Ok(opts.build())
    }
}

#[proc_macro_attribute]
pub fn worker(args: TokenStream, item: TokenStream) -> TokenStream {
    let args = parse_macro_input!(args as WorkerOpts);

    println!("args: {:?}", args);

    item
}

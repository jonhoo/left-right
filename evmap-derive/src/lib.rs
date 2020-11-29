//! This crate provides procedural derive macros to simplify the usage of `evmap`.
//!
//! Currently, only `#[derive(ShallowCopy)]` is supported; see below.
#![warn(missing_docs, rust_2018_idioms, broken_intra_doc_links)]

#[allow(unused_extern_crates)]
extern crate proc_macro;

use proc_macro2::{Ident, TokenStream};
use quote::{format_ident, quote, quote_spanned};
use syn::spanned::Spanned;
use syn::{
    parse_macro_input, parse_quote, Data, DeriveInput, Fields, GenericParam, Generics, Index,
};

/// Implementation for `#[derive(ShallowCopy)]`
///
/// evmap provides the [`ShallowCopy`](evmap::shallow_copy::ShallowCopy) trait, which allows you to
/// cheaply alias types that don't otherwise implement `Copy`. Basic implementations are provided
/// for common types such as `String` and `Vec`, but it must be implemented manually for structs
/// using these types.
///
/// This macro attempts to simplify this task. It only works on types whose members all implement
/// `ShallowCopy`. If this is not possible, consider using
/// [`CopyValue`](evmap::shallow_copy::CopyValue), `Box`, or `Arc` instead.
///
/// ## Usage example
/// ```
/// # use evmap_derive::ShallowCopy;
/// #[derive(ShallowCopy)]
/// struct Thing { field: i32 }
///
/// #[derive(ShallowCopy)]
/// struct Generic<T> { field: T }
///
/// #[derive(ShallowCopy)]
/// enum Things<T> { One(Thing), Two(Generic<T>) }
/// ```
///
/// ## Generated implementations
/// The generated implementation calls
/// [`shallow_copy`](evmap::shallow_copy::ShallowCopy::shallow_copy) on all the members of the
/// type, and lifts the `ManuallyDrop` wrappers to the top-level return type.
///
/// For generic types, the derive adds `ShallowCopy` bounds to all the type parameters.
///
/// For instance, for the following code...
/// ```
/// # use evmap_derive::ShallowCopy;
/// #[derive(ShallowCopy)]
/// struct Generic<T> { field: T }
/// ```
/// ...the derive generates...
/// ```
/// # use evmap::shallow_copy::ShallowCopy;
/// # use std::mem::ManuallyDrop;
/// # struct Generic<T> { field: T }
/// impl<T: ShallowCopy> ShallowCopy for Generic<T> {
///     unsafe fn shallow_copy(&self) -> ManuallyDrop<Self> {
///         ManuallyDrop::new(Self {
///             field: ManuallyDrop::into_inner(ShallowCopy::shallow_copy(&self.field))
///         })
///     }
/// }
/// ```
#[proc_macro_derive(ShallowCopy)]
pub fn derive_shallow_copy(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    let generics = add_trait_bounds(input.generics);
    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();
    let copy = fields(&input.data, &name);
    proc_macro::TokenStream::from(quote! {
        impl #impl_generics evmap::shallow_copy::ShallowCopy for #name #ty_generics #where_clause {
            unsafe fn shallow_copy(&self) -> std::mem::ManuallyDrop<Self> {
                #copy
            }
        }
    })
}

fn add_trait_bounds(mut generics: Generics) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(ref mut type_param) = *param {
            type_param
                .bounds
                .push(parse_quote!(evmap::shallow_copy::ShallowCopy));
        }
    }
    generics
}

fn fields(data: &Data, type_name: &Ident) -> TokenStream {
    match data {
        Data::Struct(data) => match &data.fields {
            Fields::Named(fields) => {
                let recurse = fields.named.iter().map(|f| {
                    let name = &f.ident;
                    quote_spanned! {f.span()=>
                        #name: std::mem::ManuallyDrop::into_inner(
                            evmap::shallow_copy::ShallowCopy::shallow_copy(&self.#name)
                        )
                    }
                });
                quote! {
                    std::mem::ManuallyDrop::new(Self { #(#recurse,)* })
                }
            }
            Fields::Unnamed(fields) => {
                let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                    let index = Index::from(i);
                    quote_spanned! {f.span()=>
                        std::mem::ManuallyDrop::into_inner(
                            evmap::shallow_copy::ShallowCopy::shallow_copy(&self.#index)
                        )
                    }
                });
                quote! {
                    std::mem::ManuallyDrop::new(#type_name(#(#recurse,)*))
                }
            }
            Fields::Unit => quote!(std::mem::ManuallyDrop::new(#type_name)),
        },
        Data::Enum(data) => {
            let recurse = data.variants.iter().map(|f| {
                let (names, fields) = match &f.fields {
                    Fields::Named(fields) => {
                        let field_names = f.fields.iter().map(|field| {
                            let ident = field.ident.as_ref().unwrap();
                            quote_spanned! {
                                field.span()=> #ident
                            }
                        });
                        let recurse = fields.named.iter().map(|f| {
                            let name = f.ident.as_ref().unwrap();
                            quote_spanned! {f.span()=>
                                #name: std::mem::ManuallyDrop::into_inner(
                                    evmap::shallow_copy::ShallowCopy::shallow_copy(#name)
                                )
                            }
                        });
                        (quote! { {#(#field_names,)*} }, quote! { { #(#recurse,)* } })
                    }
                    Fields::Unnamed(fields) => {
                        let field_names = f.fields.iter().enumerate().map(|(i, field)| {
                            let ident = format_ident!("x{}", i);
                            quote_spanned! {
                                field.span()=> #ident
                            }
                        });
                        let recurse = fields.unnamed.iter().enumerate().map(|(i, f)| {
                            let ident = format_ident!("x{}", i);
                            quote_spanned! {f.span()=>
                                std::mem::ManuallyDrop::into_inner(
                                    evmap::shallow_copy::ShallowCopy::shallow_copy(#ident)
                                )
                            }
                        });
                        (quote! { (#(#field_names,)*) }, quote! { (#(#recurse,)*) })
                    }
                    Fields::Unit => (quote!(), quote!()),
                };
                let name = &f.ident;
                quote_spanned! {f.span()=>
                    #type_name::#name#names => std::mem::ManuallyDrop::new(#type_name::#name#fields)
                }
            });
            quote! {
                match self {
                    #(#recurse,)*
                }
            }
        }
        Data::Union(_) => unimplemented!("Unions are not supported"),
    }
}

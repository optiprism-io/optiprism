pub trait Provider : Sync + Send{
    fn sign_up(&self, req: SignUpRequest) -> Result<TokensResponse>
}
